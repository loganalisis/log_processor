from django.urls import path
from .views import upload_log

urlpatterns = [
    path("upload/", upload_log),
]
