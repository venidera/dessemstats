# -*- coding: utf-8 -*-

"""
Copyright(C) Venidera Research & Development, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
Written by Marcos Leone Filho <marcos@venidera.com>
"""

import os
import atexit
import shutil
import logging
import tempfile
import subprocess
import distutils.cmd

from setuptools import find_packages, setup
from setuptools.command.install import install


__name__ = 'dessemstats'
__version__ = '0.0.1'
__author__ = 'Marcos Leone Filho'
__author_email__ = 'marcos@venidera.com'
__url__ = 'https://bitbucket.org/venidera_edp/dessemstats'
__description__ = 'Compute DESSEM statistics.'
__keywords__ = 'venidera miran dessem statistics'
__classifiers__ = [
    'Development Status :: 5 - Production/Stable',
    "Intended Audience :: Developers",
    "Intended Audience :: System Administrators",
    "Operating System :: Unix",
    "Topic :: Utilities"
]
__license__ = 'Proprietary'
__public_dependencies__ = ['numpy',
                           'python-dateutil',
                           'joblib',
                           'pytz',
                           'barrel_client',
                           'deckparser',
                           'XlsxWriter',
                           'vplantnaming']
__public_dependency_links__ = [
    ('git+https://git@bitbucket.org/venidera/'
    'barrel_client.git@90c2d2f3166643b49794c065f7d26b259c37cc10'
    '#egg=barrel_client'),
    'git+https://github.com/venidera/deckparser.git#egg=deckparser',
    'git+https://github.com/venidera/vplantnaming.git#egg=vplantnaming']
__private_dependencies__ = []


class PostInstallCommand(install):

    def __init__(self, *args, **kwargs):
        super(PostInstallCommand, self).__init__(*args, **kwargs)
        atexit.register(PostInstallCommand._post_install)

    @staticmethod
    def _post_install():
        # Capturing access token
        token = os.environ.get('ACCESS_TOKEN', None)
        # Creating prefix dependency urls
        prefix = 'git+https://x-token-auth:%s@bitbucket.org/venidera' % \
            token if token else 'git+ssh://git@bitbucket.org/venidera'
        # Manually installing dependencies
        for dep in __private_dependencies__:
            os.system('pip install --upgrade %s/%s.git' % (prefix, dep))
        # Removing cache and egg files
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')
        os.system('find ' + __name__ + ' | grep -E ' +
                  '"(__pycache__|\\.pyc|\\.pyo$)" | xargs rm -rf')


class PylintCommand(distutils.cmd.Command):

    user_options = [('pylint-rcfile=', None, 'path to Pylint config file')]

    def initialize_options(self):
        """ Pre-process options. """
        import requests
        # Capturing pylint file
        r = requests.get('https://drive.google.com/a/venidera.com/uc?id=' +
                         '1TLdYAyQLrxxaHtJQUFxUbrcUaolOfXcU&export=download')
        # Creating temporary repository
        repo = tempfile.mkdtemp()
        # Creating pylint file repository
        pylint = '%s/pylint.rc' % repo
        # Writing pylint file to temporary repository
        with open(pylint, 'wb') as w:
            w.write(r.content)
        # Adding custom pylint file
        self.pylint_rcfile = pylint if os.path.isfile(pylint) else ''

    def finalize_options(self):
        """ Post-process options. """
        if self.pylint_rcfile:
            assert os.path.exists(self.pylint_rcfile), (
                'Pylint config file %s does not exist.' % self.pylint_rcfile)

    def run(self):
        # Executing custom pylint
        c = subprocess.call('python setup.py lint --lint-rcfile %s' % (
            self.pylint_rcfile), shell=True)
        # Checking if call was executed with no errors
        if c != 0:
            logging.critical('Por favor, cheque seu código, pois há ' +
                             'problemas de escrita de código no padrão PEP8!')
        else:
            logging.info('Parabéns! Não foram detectados problemas ' +
                         'com a escrita do seu código.')
        # Removing the temporary directory
        shutil.rmtree('/'.join(self.pylint_rcfile.split('/')[:-1]))


# Running setup
setup(
    name=__name__,
    version=__version__,
    url=__url__,
    description=__description__,
    long_description=open('README.md').read(),
    author=__author__,
    author_email=__author_email__,
    license=__license__,
    keywords=__keywords__,
    packages=find_packages(),
    dependency_links = __public_dependency_links__,
    install_requires=[
        'pylint',
        'requests',
        'setuptools-lint'
    ] + __public_dependencies__,
    classifiers=__classifiers__,
    cmdclass={
        'pylint': PylintCommand,
        'install': PostInstallCommand
    },
    test_suite="tests"
)
