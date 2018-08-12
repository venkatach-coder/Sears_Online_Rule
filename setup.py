import init_rules
from setuptools import setup, find_packages
init_rules.init_dp_rules()

setup(name='Sears_Online_Rule',
      version='0.0.1',
      description='Sears_Online_Rule',
      author='Jianwei Xiao',
      author_email='jianwei.xiao2@searshc.com',
      packages=find_packages(),
      zip_safe=False)
