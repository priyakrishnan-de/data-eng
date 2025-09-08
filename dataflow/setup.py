import setuptools

setuptools.setup(
    name='avito-dataflow',
    version='1.0.0',
    install_requires=[
        'apache-beam[gcp]',
        'pg8000',
        'pandas',
        'fastavro'
    ],
    packages=setuptools.find_packages(),
)
