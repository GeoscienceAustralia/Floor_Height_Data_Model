from setuptools import setup, find_namespace_packages

setup(
    name='floorheights-datamodel',
    version='0.1.0',
    description='Data model and CLI app for floor height measurements',
    author='Lachlan Hurst',
    author_email='lhurst@frontiersi.com.au',
    packages=find_namespace_packages(where='src/', include=['floorheights.datamodel']),
    package_dir={'': 'src'},
    namespace_packages=['floorheights'],
    install_requires=[
        'click',
        'SQLAlchemy',
        'psycopg2-binary',
        'alembic'
    ],
    entry_points={
        'console_scripts': [
            'floorheights-dm = floorheights.datamodel.cli:main',
        ],
    },
    include_package_data=True,
)
