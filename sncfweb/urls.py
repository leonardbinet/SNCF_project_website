
from django.conf.urls import url, include
from django.contrib import admin
from . import views


urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^$', views.index, name='home'),
    url(r'^documentation/', include('documentation.urls')),
    url(r'^maps/', include('maps.urls')),
    url(r'^monitoring/', include('monitoring.urls')),
    url(r'^api/', include('project_api.urls')),
]
