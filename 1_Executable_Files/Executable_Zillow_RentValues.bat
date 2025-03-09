@echo off

@REM Save the current directory
set original_dir=%CD%

@REM Set the virtual environment root directory
set venv_root_dir="C:\Users\nickk\PycharmProjects\Data_Pipelines\venv"

@REM Navigate to the virtual environment directory
cd %venv_root_dir%

@REM Activate the virtual environment
call %venv_root_dir%\Scripts\activate.bat

@REM Navigate to the project directory
cd C:\Users\nickk\PycharmProjects\Data_Pipelines

@REM Run the Python script
python C:\Users\nickk\PycharmProjects\Data_Pipelines\ZW_RentValues.py

@REM Deactivate the virtual environment
call %venv_root_dir%\Scripts\deactivate.bat

@REM Return to the original directory
cd %original_dir%

@REM Exit with a status code
exit /B 1
