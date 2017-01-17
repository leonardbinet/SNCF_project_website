
from django.conf.urls import url, include
from django.contrib import admin
from . import views

urlpatterns = [
    url(r'^admin/', admin.site.urls),
    url(r'^$', views.index, name='home'),
    url(r'^documentation$', views.documentation, name='documentation'),
    url(r'^maps/', include('maps.urls')),
    url(r'^monitoring/', include('monitoring.urls')),

]
