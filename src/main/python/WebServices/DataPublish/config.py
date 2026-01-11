import os

vineyardsDiseasePrediction_endPoint='/prediction'

NGINX_HOST=os.getenv('NGINX_HOST', 'https://******.******.***/')
NGINX_WEB_SERVICE=os.getenv('NGINX_WEB_SERVICE', 'prediction')
NGINX_PARAMETER=os.getenv('NGINX_PARAMETER', 'weeksAgo')
NGINX_USER=os.getenv('NGINX_USER', '******')
NGINX_PASSWORD=os.getenv('NGINX_PASSWORD', '*******')

LOG_PATH = os.getenv('LOG_PATH', 'log/publish.log')