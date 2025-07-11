from setuptools import setup

setup(
    name='mario-pipeline-tools',
    version='0.54',
    packages=['mario'],
    url='https://github.com/JiscDACT/mario',
    license='all rights reserved',
    author='scottbw',
    author_email='scott.wilson@jisc.ac.uk',
    description='Base classes and helpers for common data pipeline tasks',
    long_description='A set of base classes and helpers for common data pipeline tasks, '
                     'whether using in local control scripts or in Airflow tasks.',
    package_data={
        # If any package contains *.ini files, include them
        '': ['*.ini']
    },
    include_package_data=True,
    install_requires=[
        'pandas',
        'pypika',
        'sqlalchemy',
        'openpyxl'
    ],
    extras_require={
        'Airflow': ['apache-airflow-providers-common-sql'],
        'Tableau': ['pantab', 'tableauhyperapi', 'tableau-builder==0.18']
    }
)