from django.urls import path
from . import views

urlpatterns = [
    path('', views.process_file_view, name='home'),
]