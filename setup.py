from setuptools import setup, find_packages



# Define the setup parameters
setup(
    name='hypergo',
    version='0.3.12',  # Enclose the version number in quotes
    description='Project for service bus',
    long_description_content_type='text/markdown',
    packages=find_packages(),
    install_requires=[
        "pyyaml",
	    "glom",
	    "pydash",
	    "azure-servicebus",
	    "azure-functions",
        "Click",
        "ansicolors",
        "click-default-group",
        "graphviz",
        "urllib3<2.0",
        "dill",
        "cryptography",
        "requests",
        "azure-monitor-opentelemetry",
        "opentelemetry-api",
        "opentelemetry-distro",
        "opentelemetry-exporter-otlp",
        "opentelemetry-instrumentation",
        "opentelemetry-sdk",
        "opentelemetry-instrumentation",
        "opentelemetry-resource-detector-azure",
        "opentelemetry-semantic-conventions",
        "psutil",
        "mock",
        "freezegun",
        "line-profiler"
    ],
    entry_points={
        "console_scripts": [
            "hypergo=hypergo.hypergo_click:main"
        ]
    },

)
