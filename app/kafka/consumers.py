class SearchEntry:
    def __init__(self, user_id, web_site):
        self.user_id = user_id
        self.web_site = web_site

class SearchEntryConsumer:
    def __init__(self):
        def dict_to_user(obj, ctx):
            """
            Converts object literal(dict) to a User instance.
            Args:
                ctx (SerializationContext): Metadata pertaining to the serialization
                    operation.
                obj (dict): Object literal(dict)
            """
            if obj is None:
                return None

            return SearchEntry(user_id=obj['user_id'],
                               web_site=obj['web_site'])