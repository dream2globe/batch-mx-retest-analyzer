# Jupyter Notebook to Production-Ready Spark Application

## Objective

Convert the Jupyter Notebook `notebook/retest_report.ipynb` into a production-ready Python application. This application analyzes manufacturing process data to identify high-reproducibility defect items and is designed to run on a Minikube cluster using the Spark Operator.

## Requirements

### 1. Project Structure and Code Conversion
- Analyze the existing project files, especially `notebook/retest_report.ipynb`.
- Refactor the code into a clear, role-based project structure.
  - `main.py`: Application entrypoint.
  - `app/settings.py`: Configuration management.
  - `app/spark.py`: SparkSession management.
  - `app/data_loader.py`: Data loading and preprocessing.
  - `app/analyzer.py`: Core analysis logic.
  - `app/writer.py`: Logic for writing results.
- Rename any ambiguous or unclear variables, classes, and functions to more descriptive names.

### 2. Configuration Management
- Use `pydantic-settings` for managing application configurations.
- Externalize all hardcoded configurations from `settings.py` into a `config.yml` file.
- For sensitive information (e.g., access keys, secrets):
  - Use a `.env` file for local development.
  - Ensure the `.env` file is ignored by Git.
  - In production, read credentials from Kubernetes secrets injected as environment variables.

### 3. Dependency Management
- Use `uv` as the package manager.
- Add `pyspark`, `boto3`, `pydantic-settings`, and `PyYAML` as project dependencies.
- Ensure dependencies are synchronized using `uv sync`.

### 4. Containerization and Deployment
- Create a `Dockerfile` to build a container image for the application.
  - Use `apache/spark:3.5.1` as the base image, but other suggestions are welcome.
  - The Docker image should include the application code and all necessary dependencies.
- Create a `spark-app.yaml` file to define the SparkApplication for deployment via the Spark Operator on Minikube.
- Create a `k8s-secret.yaml` template for managing sensitive credentials in the Kubernetes cluster.

### 5. Documentation
- Update the `README.md` file with the following:
  - A brief description of the project's purpose and structure.
  - Detailed instructions on how to run the project, including:
    - How to build the Docker image and push it to a container registry (e.g., JFrog).
    - How to create and manage Kubernetes secrets.
    - How to deploy the application using the Spark Operator.
