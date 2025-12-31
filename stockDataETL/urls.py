"""
URL configuration for stockDataETL project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
import test
from stockDataETL.run import InitDatabase, DailyTask, AsyncDailyTask, OneNightStockTask, OneNightStockHistoryTask


urlpatterns = [
    path('admin/', admin.site.urls),
    path('InitDatabase/', InitDatabase.initDatabase, name='InitDatabase'),
    path('DailyTask/', DailyTask.dailyTask, name='DailyTask'),
    path('AsyncDailyTask/', AsyncDailyTask.asyncDailyTask, name='AsyncDailyTask'),
    path('OneNightStockTask/', OneNightStockTask.oneNightStockTask, name='OneNightStockTask'),
    path('OneNightStockHistoryTask/', OneNightStockHistoryTask.oneNightStockHistoryTask, name='OneNightStockHistoryTask'),
    path('test', test.test, name='test'),
]
