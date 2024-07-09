from django.db import models

class Todo(models.Model):
    title = models.CharField(max_length=300)
    desc = models.CharField(max_length=300)
