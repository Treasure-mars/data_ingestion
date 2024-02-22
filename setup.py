from setuptools import setup, find_packages

setup(
    name='data_ingestion',
    version='0.1',
    packages=find_packages(exclude=['tests*']),
    license='MIT',
    description='EDSA example python package',
    install_requires=['sqlalchemy', 'logging', 'pandas'],
    url='https://github.com/Treasure-mars/data_ingestion',
    author='Treasure Mars',
    author_email='nsabitre@gmail.com'
)