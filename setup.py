from setuptools import setup, find_packages

__version__ = '1.0.0'

setup(
    name="biotreebridge",
    version=__version__,
    packages=find_packages(),
    description="",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='',
    author='nasim',
    entry_points={
        'console_scripts': ['biotreebridge = biotreebridge.cli:cli']
    },
    install_requires=[
        'charset_normalizer',
        'idna',
        'certifi',
        'requests',
        'pytest',
        'click',
        'pathlib',
        'orjson',
        'tqdm',
        'uuid',
        'openpyxl',
        'pandas',
        'inflection',
        'iteration_utilities',
        'icd10-cm',
        'beautifulsoup4',
        'gripql',
        'gen3-tracker>=0.0.7rc2',
        'fhir.resources==8.0.0b4'  # FHIR® (Release R5, version 5.0.0)
    ],
    tests_require=['pytest'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.13',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics'
    ],
    platforms=['any'],
    python_requires='>=3.13, <4.0',
)
