import datetime
import threading

import requests
from apscheduler.jobstores.base import JobLookupError
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.exceptions import ValidationError
from rest_framework.response import Response

from BetData.kafka_consumers import step_one_user_creation_reply
from BetData.kafka_producers import step_one_user_creation
from BetData.serializers import SearchSerializer, BetDataSerializer
from BetData.transaction_scheduler import transaction_scheduler, repeat_deco
from models import BetData, Search


@repeat_deco(3, reschedule_count=3, always_reschedule=True)
def rollback_function(search_id):
    # this is a query where we select the rows with that search_id, and after we delete them
    BetData.objects.filter(search=search_id).delete()
    Search.objects.filter(pk=search_id).delete()  # delete of entry search with that primary key

# In Django, a view determines the content of a web page
# views.py is where we handle the request/response logic of our web server

@api_view(['POST'])  # returns a decorator that transforms the function in a APIView REST class
def bet_data_view(request):
    user_identifier = request.data.get('user_id')
    username = request.data.get('username')
    web_site = request.data.get('web_site')
    try:
        # Step 1
        step_one_tx_id = f'step_one_user_creation_{user_identifier}'
        step_one_user_creation(username, user_identifier, step_one_tx_id)
        # Generator that contains internal infos about generator function
        step_one_user_creation_reply_gen = step_one_user_creation_reply(step_one_tx_id)
        step_one_user_creation_reply_fut = next(step_one_user_creation_reply_gen) # ends at first yield
        step_one_consumer = next(step_one_user_creation_reply_gen)
        threading.Thread(target=next, args=[step_one_user_creation_reply_gen]).start() # the rest of the generator function
                                                                                       # executed in a thread because the while True
        try:
            step_one_message = step_one_user_creation_reply_fut.result(5.0)
        except TimeoutError:
            return Response('bad', status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        
        # TODO: add response validation from step one value
        step_one_consumer.commit(step_one_message)
        step_one_consumer.close()

        # Here there's the real step 2, but with the pattern database per service we have two collection for each
        # service, so bet Search and BetData are correct in this way
        csv_url = ''
        search = SearchSerializer(data={'csv_url': csv_url, 'user': user_identifier, 'web_site': web_site})
        if search.is_valid(raise_exception=True):
            search_instance = search.save()
            # Adds the given job to the job list and wakes up the scheduler if it's already running.
            # Initiating relaxed compensating transaction (after 20 seconds)
            # misfire_grace_time is the time when the task can continue to run after the end of the deadline
            # If during a research the server shuts down and restarts, the replace_existing param allows to replace
            # the previous job with the same ID
            transaction_scheduler.add_job(rollback_function, 'date',
                                          run_date=datetime.datetime.now() + datetime.timedelta(seconds=20),
                                          args=[search_instance.pk],
                                          id=str(search_instance.pk), misfire_grace_time=3600,
                                          replace_existing=True)
            # Pausing the job to make sure that the rollback function doesn't execute during the transaction,
            # which would invalid the transaction!
            transaction_scheduler.pause_job(str(search_instance.pk))
            bet_data_list = request.data['data']
            for element in bet_data_list:
                # **element passes the entire content of the dictionary where bet_data are present
                bet_data = BetDataSerializer(data={**element, 'search': search_instance.pk})
                if bet_data.is_valid(raise_exception=True):
                    bet_data.save()

            # Step 3
            step_three_tx_id = f'step_three_user_creation_{user_identifier}'
            response_parsing_module = requests.post("http://stats_settings:8500/parsing", json=bet_data_list)
            if not response_parsing_module.ok:
                try:
                    # The 'reschedule_job' function will automatically resume the rollback job!
                    transaction_scheduler.reschedule_job(str(search_instance.pk), trigger='date')
                finally:
                    return Response('Error!', status=status.HTTP_400_BAD_REQUEST)
            else:
                associated_search_data = {'search_id': search_instance.pk, 'filename': response_parsing_module.text}
                transaction_scheduler.reschedule_job(job_id=str(search_instance.pk), trigger='date',
                                                     run_date=datetime.datetime.now() + datetime.timedelta(seconds=5))
                return Response(associated_search_data, status=status.HTTP_200_OK)

    except ValidationError:
        try:
            search_instance.pk
            transaction_scheduler.reschedule_job(job_id=str(search_instance.pk), trigger='date')
        except:
            pass
        raise
    except Exception as ex:
        try:
            search_instance.pk
            # Here we execute the rollback function (the decorator takes care of the retry logic)
            # Maybe, we could execute the job scheduler BEFORE the retry logic.
            rollback_function(search_instance.pk)
        except:
            pass

    return Response('Error!', status=status.HTTP_400_BAD_REQUEST)


@api_view(['POST'])
def url_csv_view(request):
    csv_url = request.data['url_csv']
    search_id = request.data['search_id']
    updated_search = SearchSerializer(Search.objects.get(pk=int(search_id)), data={'csv_url': csv_url}, partial=True)
    if updated_search.is_valid():
        try:
            # if in 5 seconds the job is removed, the rollback function is not called
            transaction_scheduler.remove_job(str(search_id))
        except JobLookupError:  # when the job is not found
            return Response('Job removal failed', status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        updated_search.save()
        return Response('Csv transaction success', status=status.HTTP_201_CREATED)
    else:
        return Response('Csv transaction failed, data not valid', status=status.HTTP_400_BAD_REQUEST)