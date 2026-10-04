import logging
import os
from pathlib import Path
from pathlib import PurePosixPath
import sys
from threading import Lock

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import settings
from app.schemas.prediction import (
    PredictionExplanation,
    PredictionRequest,
    PredictionResponse,
)
from app.services.explanations import (
    FEATURE_ATTRIBUTES,
    build_what_if_effects,
    build_what_if_rows,
    coalition_probabilities,
    exact_grouped_shap_values,
    grouped_global_importance,
)
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

                from src.ml.feature_engineering import (
                    feature_names_from_pipeline_model,
                    prepare_prediction_rows,
                )

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
                request_payload = request.model_dump()
                prediction_payload = {
                    key: request_payload[key]
                    for key, _ in FEATURE_ATTRIBUTES
                }
                reference_payload = {
                    "playtime_at_review": None,
                    "steam_purchase": None,
                    "received_for_free": None,
                    "is_free": None,
                    "price": None,
                    "genres": [],
                    "categories": [],
                    "platforms": {
                        "windows": None,
                        "mac": None,
                        "linux": None,
                    },
                }
                coalition_rows = []
                for mask in range(1 << len(FEATURE_ATTRIBUTES)):
                    coalition = {
                        key: (
                            prediction_payload[key]
                            if mask & (1 << index)
                            else reference_payload[key]
                        )
                        for index, (key, _) in enumerate(FEATURE_ATTRIBUTES)
                    }
                    coalition_rows.append(
                        {
                            "recommendationid": f"shap-{mask:03d}",
                            **coalition,
                        }
                    )
                what_if_rows, what_if_details = build_what_if_rows(
                    request_payload
                )
                coalition_rows.extend(what_if_rows)
                raw_frame = spark.createDataFrame(
                    coalition_rows,
                    input_schema,
                )
                feature_frame = prepare_prediction_rows(raw_frame)
                predictions = (
                    model.transform(feature_frame)
                    .select(
                        "recommendationid",
                        "prediction",
                        vector_to_array("probability")[1].alias(
                            "probability_positive"
                        ),
                    )
                    .collect()
                )
                probability_by_coalition = coalition_probabilities(
                    [
                        {
                            "recommendationid": row["recommendationid"],
                            "probability_positive": float(
                                row["probability_positive"]
                            ),
                        }
                        for row in predictions
                        if row["recommendationid"].startswith("shap-")
                    ]
                )
                shap_values = exact_grouped_shap_values(
                    probability_by_coalition
                )
                full_coalition = next(
                    row
                    for row in predictions
                    if row["recommendationid"]
                    == f"shap-{(1 << len(FEATURE_ATTRIBUTES)) - 1:03d}"
                )
                global_importance = grouped_global_importance(
                    feature_names_from_pipeline_model(model),
                    model.stages[-1].featureImportances.toArray(),
                )
                baseline_probability = probability_by_coalition[0]
                what_if_effects = build_what_if_effects(
                    [
                        {
                            "recommendationid": row["recommendationid"],
                            "probability_positive": float(
                                row["probability_positive"]
                            ),
                        }
                        for row in predictions
                        if row["recommendationid"].startswith("whatif-")
                    ],
                    what_if_details,
                    float(full_coalition["probability_positive"]),
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

        if not predictions or full_coalition["prediction"] is None:
            raise ModelUnavailableError("The prediction model returned no result")

        model_parts = PurePosixPath(model_path.replace("\\", "/")).parts
        return PredictionResponse(
            model_name=model_parts[-1] if model_parts else "unknown",
            model_run_id=model_parts[-2] if len(model_parts) > 1 else None,
            prediction=int(full_coalition["prediction"]),
            recommended=int(full_coalition["prediction"]) == 1,
            probability_positive=float(
                full_coalition["probability_positive"]
            ),
            explanation=PredictionExplanation(
                baseline_probability=baseline_probability,
                baseline_description=(
                    "Compared with a reference where each input attribute is "
                    "unspecified."
                ),
                local_shap=shap_values,
                global_importance=global_importance,
                what_if_effects=what_if_effects,
            ),
        )

    def _get_runtime(self):
        model_path = settings.ML_MODEL_PATH
        if not model_path:
            raise ModelUnavailableError(
                "Set ML_MODEL_PATH to a trained Spark MLlib PipelineModel directory"
            )

        if self._model is not None and self._loaded_path == model_path:
            return self._spark, self._model, model_path

        self._configure_hadoop_environment()

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

    @staticmethod
    def _configure_hadoop_environment() -> None:
        if os.name != "nt":
            return

        hadoop_home = settings.HADOOP_HOME or os.environ.get("HADOOP_HOME", "").strip()
        if not hadoop_home:
            raise ModelUnavailableError(
                "Set HADOOP_HOME to a Windows Hadoop directory containing "
                "bin\\hadoop.dll and bin\\winutils.exe"
            )

        home_path = Path(hadoop_home).expanduser()
        bin_path = home_path / "bin"
        if not (bin_path / "hadoop.dll").is_file():
            raise ModelUnavailableError(
                f"HADOOP_HOME does not contain bin\\hadoop.dll: {home_path}"
            )
        if not (bin_path / "winutils.exe").is_file():
            raise ModelUnavailableError(
                f"HADOOP_HOME does not contain bin\\winutils.exe: {home_path}"
            )

        os.environ["HADOOP_HOME"] = str(home_path)
        current_path = os.environ.get("PATH", "")
        path_entries = {
            entry.rstrip("\\/").casefold()
            for entry in current_path.split(os.pathsep)
            if entry
        }
        if str(bin_path).rstrip("\\/").casefold() not in path_entries:
            os.environ["PATH"] = (
                str(bin_path)
                if not current_path
                else str(bin_path) + os.pathsep + current_path
            )
