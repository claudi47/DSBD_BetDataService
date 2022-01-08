from rest_framework import serializers
from models import BetData, User, Search, Settings


# A Serializer is an object that permits to define the shape of a request/response sent by a client/server.
# Serializers allow complex data such as querysets and model instances to be converted to native Python datatypes
# that can then be easily rendered into JSON, XML or other content types.
# Lastly, it is the intermediary between the server and the db for the persistence of data
class BetDataSerializer(serializers.ModelSerializer):  # extension of the class ModelSerializer
    # This class is necessary to Django REST framework to initialize the Serializer
    class Meta:
        model = BetData
        fields = ['id', 'date', 'match', 'one', 'ics', 'two', 'gol', 'over', 'under', 'search']

#
# class UserSerializer(serializers.ModelSerializer):
#     # Creation of a property (read-only type [NO PERSISTENCE ON THE DB]) to define a relation one-to-many
#     searches = serializers.PrimaryKeyRelatedField(many=True, read_only=True)
#
#     class Meta:
#         model = User
#         fields = ['username', 'user_identifier', 'searches', 'max_research', 'ban_period']


class SearchSerializer(serializers.ModelSerializer):
    bet_data = serializers.PrimaryKeyRelatedField(many=True, read_only=True)

    class Meta:
        model = Search
        fields = ['id', 'csv_url', 'user', 'web_site', 'bet_data']


# class SettingsSerializer(serializers.ModelSerializer):
#     class Meta:
#         model = Settings
#         fields = ['goldbet_research', 'bwin_research']
#
#
# class SettingsDataSerializer(serializers.Serializer):
#     max_researches = serializers.IntegerField()
#     bool_for_all = serializers.BooleanField()
#     username_research = serializers.CharField(max_length=64, allow_null=True)
#     user_suspended = serializers.CharField(max_length=64, allow_null=True)
#     period_of_suspension = serializers.DateTimeField(allow_null=True)
#     perma_ban = serializers.BooleanField(default=False)
#     bool_toggle_goldbet = serializers.BooleanField()
#     bool_toggle_bwin = serializers.BooleanField()
#
#     def update(self, instance, validated_data):
#         raise NotImplementedError
#
#     def create(self, validated_data):
#         raise NotImplementedError
