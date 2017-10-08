from setuptools import setup

setup(
      name = "mamuto",
      version = 0.1,
      packages = ["mamuto"],
      description = "A package for distributed computation of functions over local and/or remote CPUs.",
      author = "Manuel Silva",
      author_email = "madusilva@gmail.com",
      license="GPLv2",
      classifiers=[
          "Intended Audience :: Developers",
          "Intended Audience :: Science/Research",
          "License :: OSI Approved :: GNU General Public License (GPL)",
          "Operating System :: OS Independent",
          "Programming Language :: Python",
          "Topic :: Scientific/Engineering :: Astronomy"

      ],
      install_requires = ["execnet>=1.4.1", "numpy"],
      package_data = {
          '' : []
        },
      zip_safe=False
)