from django.apps import AppConfig


class DjangoAMQPAppConfig(AppConfig):
    name = "django_amqp"
    label = "amqp"
    verbose_name = "Django AMQP"
    default_auto_field = "django.db.models.AutoField"
