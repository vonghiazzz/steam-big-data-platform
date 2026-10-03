import logging
from pathlib import Path
from pathlib import PurePosixPath
import sys
from threading import Lock

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import settings
from app.schemas.prediction import PredictionRequest, PredictionResponse
from app.services.exceptions import ModelUnavailableError


logger = logging.getLogger(__name__)


class PredictionService:
    def __init__(self) -> None:
        self._lock = Lock()
        self._spark = None
        self._model = None
        self._loaded_path: str | None = None

    def predict(self, request: PredictionRequest) -> PredictionResponse:
        with self._lock:
            spark, model, model_path = self._get_runtime()
            from py4j.protocol import Py4JJavaError
            from pyspark.errors.exceptions.base import PySparkException

            try:
                from pyspark.ml.functions import vector_to_array
                from pyspark.sql.types import (
                    ArrayType,
                    BooleanType,
                    DoubleType,
                    IntegerType,
                    StringType,
                    StructField,
                    StructType,
                )

                from src.ml.feature_engineering import prepare_prediction_rows

                platform_schema = StructType(
                    [
                        StructField("windows", BooleanType(), True),
                        StructField("mac", BooleanType(), True),
                        StructField("linux", BooleanType(), True),
                    ]
                )
                input_schema = StructType(
                    [
                        StructField("recommendationid", StringType(), False),
                        StructField("playtime_at_review", IntegerType(), True),
                        StructField("steam_purchase", BooleanType(), True),
                        StructField("received_for_free", BooleanType(), True),
                        StructField("is_free", BooleanType(), True),
                        StructField("price", DoubleType(), True),
                        StructField(
                            "genres", ArrayType(StringType(), True), True
                        ),
                        StructField(
                            "categories", ArrayType(StringType(), True), True
                        ),
                        StructField("platforms", platform_schema, True),
                    ]
                )
                payload = request.model_dump()
                payload["recommendationid"] = "inference-request"
                raw_frame = spark.createDataFrame([payload], input_schema)
                feature_frame = prepare_prediction_rows(raw_frame)
                result = (
                    model.transform(feature_frame)
                    .select(
                        "prediction",
                        vector_to_array("probability")[1].alias(
                            "probability_positive"
                        ),
                    )
                    .first()
                )
            except ModuleNotFoundError as exc:
                logger.exception("Spark MLlib runtime is unavailable")
                raise ModelUnavailableError(
                    "Spark MLlib runtime is unavailable"
                ) from exc
            except (OSError, Py4JJavaError, PySparkException) as exc:
                logger.exception("Spark prediction failed")
                raise ModelUnavailableError(
                    "The configured prediction model could not process this request"
                ) from exc

        if result is None or result["prediction"] is None:
            raise ModelUnavailableError("The prediction model returned no result")

        model_parts = PurePosixPath(model_path.replace("\\", "/")).parts
        return PredictionResponse(
            model_name=model_parts[-1] if model_parts else "unknown",
            model_run_id=model_parts[-2] if len(model_parts) > 1 else None,
            prediction=int(result["prediction"]),
            recommended=int(result["prediction"]) == 1,
            probability_positive=float(result["probability_positive"]),
        )

    def _get_runtime(self):
        model_path = settings.ML_MODEL_PATH
        if not model_path:
            raise ModelUnavailableError(
                "Set ML_MODEL_PATH to a trained Spark MLlib PipelineModel directory"
            )

        if self._model is not None and self._loaded_path == model_path:
            return self._spark, self._model, model_path

        try:
            from py4j.protocol import Py4JJavaError
            from pyspark.errors.exceptions.base import PySparkException
            from pyspark.ml import PipelineModel
            from pyspark.sql import SparkSession
        except ModuleNotFoundError as exc:
            logger.exception("Spark MLlib runtime is unavailable")
            raise ModelUnavailableError(
                "Spark MLlib runtime is unavailable"
            ) from exc

        try:
            builder = (
                SparkSession.builder.appName("Steam ML Prediction API")
                .master(settings.SPARK_MASTER)
                .config("spark.sql.session.timeZone", "UTC")
            )
            if settings.HDFS_DEFAULT_FS:
                builder = builder.config(
                    "spark.hadoop.fs.defaultFS", settings.HDFS_DEFAULT_FS
                )
            spark = builder.getOrCreate()
            spark.sparkContext.setLogLevel("WARN")
            model = PipelineModel.load(model_path)
        except ModuleNotFoundError as exc:
            logger.exception("Spark MLlib runtime is unavailable")
            raise ModelUnavailableError(
                "Spark MLlib runtime is unavailable"
            ) from exc
        except OSError as exc:
            logger.exception("Configured Spark MLlib model path is unavailable")
            raise ModelUnavailableError(
                "The configured prediction model is unavailable"
            ) from exc
        except (Py4JJavaError, PySparkException) as exc:
            logger.exception("Could not load the configured Spark MLlib model")
            raise ModelUnavailableError(
                "The configured prediction model is unavailable"
            ) from exc

        self._spark = spark
        self._model = model
        self._loaded_path = model_path
        return spark, model, model_path
