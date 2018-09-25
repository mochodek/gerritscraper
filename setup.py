from setuptools import setup

setup(name='gerritscraper',
      version='1.0',
      description='Gerrit scraper',
      url='https://github.com/mochodek/gerritscraper',
      author='',
      author_email='',
      license='Apache-2.0',
      packages=['gerrit'],
      install_requires=[
          'pygerrit2',
          'pathos',
          'pymongo'
      ],
      scripts=[
          'bin/android_gerrit',
          'bin/aospa_gerrit',
          'bin/chromium_gerrit',
          'bin/eclipse_gerrit',
          'bin/kitware_gerrit',
          'bin/libreoffice_gerrit',
          'bin/openstack_gerrit',
          'bin/ovirt_gerrit',
          'bin/romandev_chromium_gerrit',
          'bin/spdk_gerrit',
          'bin/wikimedia_gerrit'],
      zip_safe=False)
