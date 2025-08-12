from dataclasses import dataclass


@dataclass
class SparkEngine:
    app_name: str
    master: str
    instance_num: int
    executor_memory: int
    executor_core: int
    driver_memory: str
    driver_max_result: str
    shuffle_partitions: int


@dataclass
class TrinoEngine:
    host: str
    port: str
    user: int
    catalog: int
    schema: int


@dataclass
class ObjectStorage:
    access_key: str
    secret_key: str
    endpoint_url: str


@dataclass
class AppConfig:
    proxy_ip: str
    bucket_name: str
    storage: ObjectStorage
    engine: SparkEngine


def load_config(file_path: str, engine_type: str, site: str) -> AppConfig:
    with open(file_path, "rb") as file:
        conf = tomli.load(file)
    storage = ObjectStorage(
        access_key=conf["gage"]["access_key"],
        secret_key=conf["gage"]["secret_key"],
        endpoint_url=conf["gage"]["endpoint_url"],
    )
    if engine_type == "spark":
        query_engine = SparkEngine(
            app_name=conf["spark"]["app_name"],
            master=conf["spark"]["master"],
            instance_num=conf["spark"]["instance_num"],
            executor_memory=conf["spark"]["executor_memory"],
            executor_core=conf["spark"]["executor_core"],
            driver_memory=conf["spark"]["driver_memory"],
            driver_max_result=conf["spark"]["driver_max_result"],
            shuffle_partitions=conf["spark"]["shuffle_partitions"],
        )
    elif engine_type == "trino":
        query_engine = TrinoEngine(
            host=conf["trino"]["host"],
            port=conf["trino"]["port"],
            user=conf["trino"]["user"],
            catalog=conf["trino"]["catalog"],
            schema=conf["trino"]["schema"],
        )
    else:
        raise AttributeError("engine_type parameter should be set 'spark' or 'trino'")

    return AppConfig(
        proxy_ip=conf["common"]["proxy_ip"],
        bucket_name=conf["bucket"][site],
        storage=storage,
        engine=query_engine,
    )


# if __name__ == "__main__":
#    config = load_config("config.toml")
#    print(config)
