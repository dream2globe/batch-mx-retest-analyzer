from pathlib import Path

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def load_yaml_config(file_path: Path):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)


config_path = Path(__file__).parent.parent / "config.yml"
yaml_config = load_yaml_config(config_path)


class SparkSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SPARK_")

    master: str = Field(default=yaml_config["spark"]["master"])
    app_name: str = Field(default=yaml_config["spark"]["app_name"])
    driver_host: str = Field(default=yaml_config["spark"]["driver_host"])
    driver_port: str = Field(default=yaml_config["spark"]["driver_port"])
    ui_port: str = Field(default=yaml_config["spark"]["ui_port"])
    executor_instances: int = Field(default=yaml_config["spark"]["executor_instances"])
    executor_cores: int = Field(default=yaml_config["spark"]["executor_cores"])
    executor_memory: str = Field(default=yaml_config["spark"]["executor_memory"])
    driver_memory: str = Field(default=yaml_config["spark"]["driver_memory"])
    jars_packages: str = Field(default=yaml_config["spark"]["jars_packages"])
    sql_extensions: str = Field(default=yaml_config["spark"]["sql_extensions"])
    sql_catalog_spark_catalog: str = Field(
        default=yaml_config["spark"]["sql_catalog_spark_catalog"]
    )


class S3Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="S3_")

    endpoint: str = Field(default=yaml_config["s3"]["endpoint"])
    access_key: str = Field(default=yaml_config["s3"]["access_key"])
    secret_key: str = Field(default=yaml_config["s3"]["secret_key"])
    bucket: str = Field(default=yaml_config["s3"]["bucket"])


class AppSettings(BaseSettings):
    factory: str = Field(default=yaml_config["app"]["factory"])
    top_codes: list[str] = Field(default=yaml_config["app"]["top_codes"])
    model_count: int = Field(default=yaml_config["app"]["model_count"])
    start_date: str = Field(default=yaml_config["app"]["start_date"])
    end_date: str = Field(default=yaml_config["app"]["end_date"])
    start_duration: str = Field(default=yaml_config["app"]["start_duration"])
    end_duration: str = Field(default=yaml_config["app"]["end_duration"])


class Settings(BaseSettings):
    spark: SparkSettings = Field(default_factory=SparkSettings)
    s3: S3Settings = Field(default_factory=S3Settings)
    app: AppSettings = Field(default_factory=AppSettings)


settings = Settings()
