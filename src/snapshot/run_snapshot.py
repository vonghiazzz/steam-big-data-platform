from create_manifest import create_manifest



def main():


    gold_paths = {

        "base":
        "hdfs://namenode:8020/steam/gold/base",


        "game_aggregation":
        "hdfs://namenode:8020/steam/gold/game_aggregation"

    }


    create_manifest(

        dataset_version="v1.0",

        gold_paths=gold_paths,

        output_file= "/tmp/dataset_manifest_v1.json"

    )


if __name__=="__main__":

    main()