
# Ingeniero: Andrés Felipe Rojas Parra
# Maestria en Big Data y Data Science
# 2024 - 2025
#
# Descripcion del script:
# La solucion de computer vision se debe ejecutar en python 3.10
#
# Este Makefile permite crear el ambiente virtual, instala prerrequisitos, realiza pruebas de los archivos, detecta 
# la plataforma donde se esta ejecutando el Makefile (windows, jetson, ubuntu) y genera variables internas necesarias para la correcta
# ejecucion del Makefile
#
# Antes de correr el Makefile se debe revisar la configuracion del parametro SOLUTION, PYTHON_VERSION
#
# Para ejecutar el Makefile, en las reglas validate create-venv install config-env all, se debe pasar los parametros FWORK PVENV
#
# FWORK debe ser tf (tensorflow) o py (pytorch)
# PVENV se debe poner la ruta completa donde se quiere crear el ambiente virtual
#
# Ejemplo de la regla validate: make validate FWORK=tf PVENV=/media/arojaspa/Data/dev/environments/ubuntu
# Ejemplo de la regla create-venv: make validate FWORK=tf PVENV=/media/arojaspa/Data/dev/environments/ubuntu
# Ejemplo de la regla install: make validate FWORK=tf PVENV=/media/arojaspa/Data/dev/environments/ubuntu
# Ejemplo de la regla config-env: make validate FWORK=tf PVENV=/media/arojaspa/Data/dev/environments/ubuntu
# Ejemplo de la regla all: make validate FWORK=tf PVENV=/media/arojaspa/Data/dev/environments/ubuntu
#
# Para las demas reglas, no es necesario los parametros FWORK y PVENV

# =========================
# Nombre de la aplicacion
# =========================
SOLUTION := cursoetl_

# =========================
# VERSION DE PYTHON
# =========================
PYTHON_VERSION := 3.10
PYTHON_BIN     := python$(PYTHON_VERSION)

# =========================
# DETECCIÓN DE MÁQUINA
# =========================
ifeq ($(OS),Windows_NT)
MACHINE = WIN32
  ifeq ($(PROCESSOR_ARCHITECTURE),AMD64)
    MACHINE += AMD64
  endif
  ifeq ($(PROCESSOR_ARCHITECTURE),x86)
    MACHINE += IA32
  endif
else
UNAME_S := $(shell uname -s)
  ifeq ($(UNAME_S),Linux)
    MACHINE = LINUX
  endif
  ifeq ($(UNAME_S),Darwin)
    MACHINE = OSX
  endif

UNAME_P := $(shell uname -p)
  ifeq ($(UNAME_P),x86_64)
    MACHINE += x86_64
  endif
  ifneq ($(filter %86,$(UNAME_P)),)
    MACHINE += IA32
  endif
  ifneq ($(filter arm% aarch64,$(UNAME_P)),)
    MACHINE += JETSON
  endif
endif

# =========================
# CLASIFICAR PLATAFORMA
# PLATFORM = ubuntu_pc | jetson | windows | unknown
# =========================
ifeq ($(findstring WIN32,$(MACHINE)),WIN32)
PLATFORM := windows
else ifeq ($(words $(filter LINUX x86_64,$(MACHINE))),2)
PLATFORM := ubuntu_pc
else ifneq ($(findstring JETSON,$(MACHINE)),)
PLATFORM := jetson
else
PLATFORM := unknown
endif


# =========================
# VALIDAR FWORK / PVENV SOLO PARA CIERTOS TARGETS
# =========================

# Targets que SÍ necesitan que FWORK y PVENV estén definidos
NEED_ENV_TARGETS := validate create-venv install config-env all

# Si el usuario llamó a alguno de esos targets, entonces validamos
ifneq ($(filter $(NEED_ENV_TARGETS),$(MAKECMDGOALS)),)
# =========================
# VALIDAR FWORK (tf | py)
# =========================
  ifeq ($(FWORK),)
  	$(error You must define FWORK=tf or FWORK=py. Sample: make install FWORK=tf PVENV=/ruta/...)
	endif

# =========================
# VALIDAR RUTA AMBIENTE VIRTUAL
# =========================
  ifeq ($(PVENV),)
  	$(error You must define PVENV. Sample: make install PVENV=/media/arojaspa/Data/dev/environments)
	endif

# =========================
# ASIGNAR REQUIREMENTS Y ENV_PATH
# =========================
REQUIREMENTS_FILE :=
ENV_PATH          :=
ENV_NAME          :=

	# --- Caso TensorFlow ---
	ifeq ($(FWORK),tf)

	ifeq ($(PLATFORM),ubuntu_pc)
		REQUIREMENTS_FILE := requirements_tf_ubuntu.txt
		ENV_NAME          := tf_ubuntu
	else ifeq ($(PLATFORM),jetson)
		REQUIREMENTS_FILE := requirements_tf_jetson.txt
		ENV_NAME          := tf_jetson
	else ifeq ($(PLATFORM),windows)
		REQUIREMENTS_FILE := requirements_tf_windows.txt
		ENV_NAME          := tf_windows
	else
		$(error Platform $(PLATFORM) is not supported for FWORK=tf)
	endif

	# --- Caso PyTorch ---
	else ifeq ($(FWORK),py)

	ifeq ($(PLATFORM),ubuntu_pc)
		REQUIREMENTS_FILE := requirements_py_ubuntu_pc.txt
		ENV_NAME          := py_ubuntu
	else ifeq ($(PLATFORM),jetson)
		REQUIREMENTS_FILE := requirements_py_jetson.txt
		ENV_NAME          := py_jetson
	else ifeq ($(PLATFORM),windows)
		REQUIREMENTS_FILE := requirements_py_windows.txt
		ENV_NAME          := py_windows
	else
		$(error Platform $(PLATFORM) is not supported for FWORK=py)
	endif

	else
	$(error FWORK value is not valid: use tf or py)
	endif

	ENV_PATH := $(PVENV)/$(SOLUTION)$(ENV_NAME)
endif

# =========================
# VENV PYTHON PATH (para validación)
# =========================
ifeq ($(PLATFORM),windows)
VENV_PY := $(ENV_PATH)/Scripts/python.exe
else
VENV_PY := $(ENV_PATH)/bin/python
endif

# =========================
# TARGETS
# =========================

.PHONY: validate
validate:
	@echo "Platform: $(PLATFORM)"
	@echo "Python version: $(PYTHON_VERSION)"
	@echo "Using $(MACHINE) Architecture"
	@echo "Setting up the environment for: $(FWORK)"
	@echo "PVENV: $(PVENV)"
	@echo "Requirements file to be used: $(REQUIREMENTS_FILE)"
	@echo "The venv will be created at: $(ENV_PATH)"

.PHONY: create-venv
create-venv: validate
	@echo "Creating VENV at: $(ENV_PATH)"
	@$(PYTHON_BIN) -m venv "$(ENV_PATH)"
	@if [ ! -x "$(VENV_PY)" ]; then \
		echo "ERROR: venv not created correctly at $(ENV_PATH)"; \
		exit 1; \
	fi
	@echo "VENV created OK at: $(ENV_PATH)"

.PHONY: install
install: create-venv
	@echo "Requirements file used: $(REQUIREMENTS_FILE)"
	@. "$(ENV_PATH)/bin/activate" && pip install --upgrade pip setuptools wheel && pip install -r "$(REQUIREMENTS_FILE)"

.PHONY: config-env
config-env:
	@echo "Requirements file used: $(REQUIREMENTS_FILE)"
	@. "$(ENV_PATH)/bin/activate" && pip install --upgrade pip setuptools wheel && pip install -r "$(REQUIREMENTS_FILE)"

.PHONY: test
test:
	python -m pytest -vvv --cov=hello --cov=greeting --cov=smath --cov=web tests
	python -m pytest --nbval notebook.ipynb
	# python -m pytest -v tests/test_web.py

.PHONY: debug
debug:
	python -m pytest -vv --pdb

.PHONY: one-test
one-test:
	python -m pytest -vv tests/test_greeting.py::test_my_name4

.PHONY: debugthree
debugthree:
	# not working the way I expect
	python -m pytest -vv --pdb --maxfail=4  # drop to PDB for first three failures

.PHONY: format
format:
	# used to format the file code 
	black codigobase procesosbatch

.PHONY: lint
lint:
	pylint --disable=R,C *.py

.PHONY: all
all: install lint test format
