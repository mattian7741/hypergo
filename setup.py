from setuptools import setup



# Define the setup parameters
setup(
    name='hypergo',
    version='0.1.0',  # Enclose the version number in quotes
    description='Project for service bus',
    long_description_content_type='text/markdown',
    packages=['hypergo'],
    install_requires=[
        "pyyaml",
	    "glom",
	    "pydash",
	    "azure-servicebus",
	    "azure-functions",
        'Click',
        'ansicolors',
        'click-default-group',
        'graphviz',
        'urllib3<2.0',
        'dill',
        'cryptography'
    ],
    entry_points={
        'console_scripts': [
            'hypergo=hypergo.hypergo_click:main'
        ]
    },

)