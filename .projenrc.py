from projen import ProjectType
from projen.python import PythonProject
from projen.release import Release

project = PythonProject(
    author_email="rix0rrr@gmail.com",
    author_name="Rico Hermans",
    module_name="qnddb",
    name="qnddb",
    version="0.1.0",
    project_type=ProjectType.LIB,
    deps=[
        'boto3@^1.26.109',
    ],
    dev_deps=[
        'hypothesis@^6.71.0',
    ],
)
project.gitignore.add_patterns('.DS_Store')

rls = Release(project, artifacts_directory='dist', branch='main', task=project.build_task, version_file='version.json')
rls.publisher.publish_to_py_pi()

project.synth()