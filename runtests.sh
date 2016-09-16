#!/bin/bash

coverage erase
coverage run --source=filemerge --branch -m unittest discover unit_tests
coverage html
coverage report