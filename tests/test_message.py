import unittest

import azure.functions as func

from hypergo.message import Message


class TestMessage(unittest.TestCase):
    def test_from_http_request_happy_path(self) -> None:
        http_request = func.HttpRequest(
                method='POST',
                body='{"good": "json"}'.encode('utf-8'),
                url='https://bgray-test.azurewebsites.net/api/HttpRequestHandler/other_tag?code=-i6NwCuDbR5-rjzNUt7w6hdyjti_5Ccs-yMoaIamIFMmAzFuPk3Xhw==',
                params=None)
        result = Message.from_http_request(http_request)

        self.assertEqual(result['body'],  {'good': 'json'})
        self.assertEqual(result['routingkey'], 'http_request.api.HttpRequestHandler.other_tag')

if __name__ == '__main__':
    # Run the unit tests
    unittest.main()
