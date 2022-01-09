from django.urls import path

from BetData.views import bet_data_view, url_csv_view

urlpatterns = [
    path('betdata/', bet_data_view),
    path('csv/', url_csv_view)
]