#!/usr/bin/env python
import os
import sys

if __name__ == "__main__":
    # TODO(bdarnell): find a cleaner way to do this
    sys.path.append('../../..')
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "mysite.settings")

    from django.core.management import execute_from_command_line

    execute_from_command_line(sys.argv)
