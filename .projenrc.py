from projen.python import PythonProject

project = PythonProject(
    author_email="rix0rrr@gmail.com",
    author_name="Rico Huijbers",
    module_name="qnddb",
    name="qnddb",
    version="0.1.0",
)

project.synth()