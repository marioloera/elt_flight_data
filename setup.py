import setuptools

REQUIRED_PACKAGES = [
    "apache-beam[gcp]==2.37.0",
]

EXTRA_PACKAGES = {
    "dev": [
        "coverage==5.2.1",
        "flake8==3.8.3",
        "pre-commit==2.7.1",
        "pytest==6.2.2",
    ]
}

setuptools.setup(
    name="etl_flight_data",
    version="0.0.1",
    description="etl_flight_data pipeline",
    install_requires=REQUIRED_PACKAGES,
    extras_require=EXTRA_PACKAGES,
    packages=setuptools.find_packages(),
    author="Mario Loera",
    author_email="marioll@kth.se",
    url="https://github.com/marioloera/elt_flight_data",
)
