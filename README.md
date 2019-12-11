

# Analisador de resultados do DESSEM

![GitHub Release](https://img.shields.io/badge/release-v0.0.1-blue.svg)
![GitHub license](https://img.shields.io/badge/license-Proprietary-yellow.svg)

Este é um tutorial rápido de como se utilizar e programar este pacote de software para fazer análises de rodadas do DESSEM.

## Table of contents
[TOC]

## Requirements

* Python 3.6 or superior (`sudo apt-get install python3.6` under Linux)
* Python virtual environment (`sudo apt-get install python3.6-venv` under Linux)
* Git

## Package structure

Estrutura de diretórios e arquivos:

- `dessemstats` Diretório raiz do pacote.
    -   `README.md`  Este documento - deve ser escrito em Markdown.
    -   `setup.py` Script para compilar e instalar este pacote.
    -   `LICENSE` Documento de licença.
    -   `.gitignore` Especifica arquivos que não serão monitorados pelo git.
    -   `dessemstats` Módulo base do pacote.
        -   `compare_dessem_sagic.py`  Módulo principal do pacote que computa os indicadores do DESSEM.
    -   `scripts` Diretório básico com os scripts do usuário.
        -   `run.py` Um script básico de como rodar esta aplicação.
    -   `tests` Coleção de testes de uso geral.
        -   `base_test.py` Teste mínimo de sanidade para o módulo  `compare_dessem_sagic.py`.

## Development and Tests

### 1. Cloning the repository
The first step aims to clone the `dessemstats` repository from Bitbucket. It is possible to change the current working directory to the location where you want the cloned directory to be made. Note that `{my.bitbucket.username}` below must be replaced with your bitbucket username.

```bash
$ mkdir ~/git
$ git clone https://{my.bitbucket.username}@bitbucket.org/venidera_edp/dessemstats.git ~/git/dessemstats
$ cd ~/git/dessemstats
```

### 2. Installing the package
 Start by creating a new virtual environment for your project. Next, update the packages `pip` and `setuptools` to the latest version. Then install the package itself.
```bash
$ /usr/bin/python3.6 -m venv --prompt="dessemstats" venv
$ source venv/bin/activate
(dessemstats) $ pip install --upgrade setuptools pip
(dessemstats) $ python setup.py install
```

### 3. Code checking
It is also possible to check for errors in Python code using:
```bash
(dessemstats) $ python setup.py pylint
```
Pylint is a tool that tries to enforce a coding standard and looks for  [code smells](https://martinfowler.com/bliki/CodeSmell.html). Pylint will display several messages as it analyzes the code and it can also be used for displaying some statistics about the number of warnings and errors found in different files. The current package uses a custom [configuration file](https://drive.google.com/a/venidera.com/uc?id=1SeUYS-g-MTj-7a_XYwaXZUQpDiQ26JuW), tailored to Venidera code standard.


### 4. Testing package modules
Python tests are Python classes that reside in separate files from the code being tested. In this project, module tests are based on Python `unittest` package and are located in the `tests` directory. They can be run by the following code: 
```bash
(dessemstats) $ python setup.py test
```
In general, the developer can create and perform as many tests as he needs. However, it is important to validate them before committing a new change to the Bitbucket Cloud, as a way of avoiding errors. It is also important to mention that tests will only be performed if test classes extend the `unittest.TestCase` object.


### 5. Running the application
To run your package (and also to generate a script that helps other developers to execute your package), put your package's execution routines into `scripts/run.py` directory. Then, once package syntax is following Venidera code standards and all tests were performed, you can run the application by executing the following code:
```bash
(dessemstats) $ python scripts/run.py
```

## Troubleshooting

Please file a BitBucket issue to [report a bug](https://bitbucket.org/venidera_edp/dessemstats/issues?status=new&status=open).

## Maintainers

-   Marcos Leone Filho - [marcos@venidera.com](mailto:marcos@venidera.com)

## License

This package is released and distributed under the license  [GNU GPL Version 3, 29 June 2007](https://www.gnu.org/licenses/gpl-3.0.html).