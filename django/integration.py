from core.models import Product, Role, Variation, VariationRole, ProductRole, Order
from rest_framework.response import Response
from rest_framework.decorators import api_view
from core.views.page_data import set_paginator
from django.core.management import call_command
from background_task import background
from typing import Union, Type
from datetime import datetime
from requests.auth import HTTPBasicAuth
from django.conf import settings
import json
import requests
import logging


def get_product_variation_data(product_item: Product, role_items: dict, role_relation: str,
                               variation_item: Variation = None):
    """ Product or variation data """

    item_data = {
        "product_id": product_item.id,
        "variation_id": variation_item.id if variation_item else None,
        "title": product_item.title,
        "price": variation_item.price if variation_item else product_item.price,
        "discount_price": variation_item.discount_price if variation_item else product_item.discount_price
    }
    if variation_item:
        if variation_item.attributes:
            attribute_data = str()
            for attribute_item in variation_item.attributes.all():
                attribute_value = str()
                if attribute_item.value:
                    attribute_value = attribute_item.value
                elif attribute_item.color_id:
                    attribute_value = attribute_item.color_id.title
                attribute_data += f" {attribute_value}"
            item_data["title"] = f'{product_item.title}:{attribute_data}'
    for role_item in role_items:
        item_data[role_item.slug] = {"price": None, "discount_price": None}

    get_role_items = getattr(variation_item or product_item, role_relation).all()
    if get_role_items:
        for product_role_item in get_role_items:
            item_data[product_role_item.role_id.slug] = {"price": product_role_item.price,
                                                         "discount_price": product_role_item.discount_price}
    return item_data


@api_view(['GET'])
def product_list(request):
    """ Get product and variations list """
    product_items = Product.objects.prefetch_related('product_variation__variation_role',
                                                     'product_variation__attributes__color_id').filter(
        is_show=True).all().order_by('id')
    result = list()
    product_items, pages = set_paginator(product_items, 10, request.GET.get('page'))
    role_items = Role.objects.all()
    for product_item in product_items:
        if product_item.is_variation():
            for variation_item in product_item.product_variation.all():
                result.append(get_product_variation_data(product_item, role_items, 'variation_role', variation_item))
        else:
            result.append(get_product_variation_data(product_item, role_items, 'productrole_set'))
    return Response({"result": result, "pages": pages})


def update_object(product_id: int, item: dict, get_role_items: dict, role_relation: str,
                  inst: Type[Union[Product, Variation]]):
    """ Updating price for products and variations including roles prices """

    try:
        object_item = inst.objects.get(id=product_id)
        if item.get('price'):
            object_item.price = item.get('price')
        if item.get('discount_price'):
            object_item.discount_price = item.get('discount_price')
        object_item.save()
        for get_role_item in get_role_items:
            if get_role_item.slug in item:
                getattr(object_item, role_relation).update_or_create(role_id__slug=get_role_item.slug,
                                                                     role_id=get_role_item,
                                                                     defaults=item[get_role_item.slug])
        return {'result': object_item}
    except inst.DoesNotExist:
        error_message = f"product_id: {item.get('product_id')}, 'variation_id: {item.get('variation_id')} - product or variation not found"
        return {'error_message': error_message}


@api_view(['PUT'])
def change_prices(request):
    """ Change product price and variation price """
    get_data = json.loads(request.body)
    get_role_items = Role.objects.all()
    error_counter = 0
    success_counter = 0
    error_message = list()
    production_logging = logging.getLogger('production')
    for item in get_data:
        if item.get('variation_id'):
            result = update_object(item.get('variation_id'), item, get_role_items, 'variation_role', Variation)
        else:
            result = update_object(item.get('product_id'), item, get_role_items, 'productrole_set', Product)
        if result.get('error_message'):
            error_counter += 1
            error_message.append(result.get('error_message'))
        else:
            success_counter += 1
    calculate_price()
    if error_message:
        production_logging.error(f"{datetime.now()} - error: {error_message}")
    return Response({'code': 202, 'success': success_counter, 'error': error_counter, 'error_message': error_message})


@background(schedule=60)
def calculate_price():
    call_command('seeder')


def send_request_to_server(url, data=None, headers=None, login=None, password=None, method='get'):
    """ Send request to 1C"""
    host = settings.HOST_1C if hasattr(settings, 'HOST_1C') else None
    url = host + url
    if not login:
        login = settings.LOGIN_1C if hasattr(settings, 'LOGIN_1C') else None
    if not password:
        password = settings.PASSWORD_1C if hasattr(settings, 'PASSWORD_1C') else None
    auth = HTTPBasicAuth(login, password) if login and password else None
    if method == 'post':
        response = requests.post(url, headers=headers, json=data, auth=auth)
    else:
        response = requests.get(url, headers=headers, json=data, auth=auth)
    if response.status_code == 500:
        logging.error(response.status_code)
        logging.error(data)
        logging.error(response.text)
    return response


def send_order_to_server(order_id):
    """ Send order to 1C """
    collect_data = dict()
    headers = {
        'Content-Type': 'application/json',
        'X-WC-Webhook-Source': 'https://skyprofil.by/',
    }
    try:
        order_item = Order.objects.prefetch_related('order_cart').get(id=order_id)
        collect_data['id'] = order_item.id
        collect_data['date_created'] = str(order_item.date_order).replace(' ', 'T')
        collect_data['total'] = str(order_item.total())
        collect_data['shipping_total'] = str(order_item.delivery_price) if order_item.delivery_price else None
        collect_data['customer_id'] = None
        collect_data['customer_note'] = order_item.additional_information
        collect_data['shipping'] = dict()
        collect_data['shipping'].update({
            'address_1': order_item.delivery_address
        })
        collect_data['billing'] = dict()
        collect_data['billing'].update({
            'first_name': order_item.fio,
            'email': order_item.email,
            'phone': order_item.phone
        })
        if order_item.filial_id:
            collect_data['shipping_lines'] = list()
            if order_item.delivery_address:
                filial_address = "Доставка из " + order_item.delivery_address
            else:
                filial_address = "Самовывоз из " + order_item.filial_id.address
            collect_data['shipping_lines'].append({
                'method_title': filial_address
            })
        collect_data['line_items'] = list()
        for cart_item in order_item.order_cart.all():
            collect_data['line_items'].append({
                'quantity': cart_item.quantity,
                'total': str(cart_item.total_price()),
                'variation_id': cart_item.variation_id_id,
                'product_id': cart_item.product_id_id
            })
        return send_request_to_server('/skyprofil-noauth/hs/wcwhv2/order.created', collect_data,
                                      headers=headers, method='post')
    except Order.DoesNotExist:
        pass
