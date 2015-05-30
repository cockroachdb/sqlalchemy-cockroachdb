FROM python:3-onbuild

CMD python -m unittest discover -b -v
