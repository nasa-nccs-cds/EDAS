from distutils.core import setup
setup(
  name = 'pyedas',
  packages = ['pyedas','pyedas.kernels','pyedas.kernels.internal','pyedas.portal','pyedas.eofs'],
  version = '1.2.1',
  package_dir = {'': 'python/src'},
  description = 'Python portal and worker components of the Earth Data Analytic Services (EDAS) framework',
  author = 'Thomas Maxwell',
  author_email = 'thomas.maxwell@nasa.gov',
  url = 'https://github.com/nasa-nccs-cds/EDAS.git',
  download_url = 'https://github.com/nasa-nccs-cds/EDAS/tarball/1.2.1',
  keywords = ['climate', 'data', 'analytic', 'services'],
  license = 'GNU GENERAL PUBLIC',
  classifiers = [],
)