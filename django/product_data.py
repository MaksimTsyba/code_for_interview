from core.serializers import ProductSerializer
from core.views.shop.shop import Product
from django.http import Http404
from core.models import SEOSetting
from core.views.seo_setting_data import SEOSettingsData
import logging


class ProductData:
    """Product data"""

    def __init__(self, product=None, slug=None, fields=None, additional_fields=None, prefetch_related=None,
                 user_role=None):
        self.slug = slug
        self.product_item = product
        self.prefetch_related = prefetch_related or []
        self.fields = fields
        self.additional_fields = additional_fields
        self.product = self._get_product()
        self.user_role = user_role
        self.exclude_fields = ['min_price', 'max_price', 'old_min_price', 'old_max_price', 'productrole_set']

    def _get_product(self):
        """ Get product from db or like item"""
        if self.slug:
            try:
                return Product.objects.prefetch_related(*self.prefetch_related).get(slug=self.slug)
            except Product.DoesNotExist:
                raise Http404
        if self.product_item:
            return self.product_item
        raise Http404

    @staticmethod
    def _get_seo_settings():
        """ Get seo settings """
        seo_setting_items = SEOSetting.objects.all()
        seo_setting = {}
        for seo_setting_item in seo_setting_items:
            seo_setting[seo_setting_item.key_name] = seo_setting_item.value
        return seo_setting

    def _get_meta_title(self, seo_setting):
        """ Generate meta title """
        meta_title = self.product.title
        if self.product.seo_title:
            meta_title = self.product.seo_title
        else:
            if seo_setting.get('product_meta_title_additional'):
                meta_title = seo_setting.get('product_meta_title_additional').replace('%title%', self.product.title)
                if seo_setting.get('sep'):
                    meta_title = meta_title.replace('%sep%', seo_setting.get('sep'))
                if seo_setting.get('site_name'):
                    meta_title = meta_title.replace('%sitename%', seo_setting.get('site_name'))

        return meta_title

    def _get_meta_description(self, seo_setting):
        """ Generate meta descriptions """
        meta_description = self.product.seo_description
        if seo_setting.get('product_meta_description') and not meta_description:
            meta_description = seo_setting.get('product_meta_description').replace('%title%', self.product.title)
        return meta_description

    def get_variation_price(self):
        """ Get variation price with check role """
        variation_price = {
            'min_price': self.product.min_price,
            'max_price': self.product.max_price,
            'old_min_price': self.product.old_min_price,
            'old_max_price': self.product.old_max_price
        }
        if self.user_role:
            for role_item in self.product.productrole_set.all():
                if role_item.role_id_id == self.user_role.pk:
                    if role_item.min_price:
                        variation_price['min_price'] = role_item.min_price
                    if role_item.max_price:
                        variation_price['max_price'] = role_item.max_price
                    if role_item.old_min_price:
                        variation_price['old_min_price'] = role_item.old_min_price
                    if role_item.old_max_price:
                        variation_price['old_max_price'] = role_item.old_max_price
        return variation_price

    def get_url(self):
        """ Get product url """
        get_category = self.product.main_category
        if get_category:
            get_menu = get_category.menu_category_set.all()
            if get_menu:
                get_menu = get_menu[0]
                watcher = 1
                menu_parent = get_menu
                result = get_menu.get_slug() + '/'
                while watcher == 1:
                    if menu_parent.parent:
                        parent_slug = menu_parent.parent.get_slug()
                        result = parent_slug + '/' + result
                        menu_parent = menu_parent.parent
                    else:
                        watcher = 0
                result = '/' + result
                return result
        return None

    def _get_price_by_user_role(self, ):
        """ Get price by role """
        product_price = dict()

        if self.user_role:
            for role_item in self.product.productrole_set.all():
                if role_item.role_id_id == self.user_role.pk:
                    price = role_item.discount_price or role_item.price
                    if price:
                        product_price['price'] = price
                        if role_item.discount_price:
                            product_price['old_price'] = role_item.price
            return product_price

    def get_price(self):
        """ Get product price """
        price = self.product.discount_price or self.product.price
        role_price = self._get_price_by_user_role()
        if role_price:
            price = role_price['price'] or price

        return price

    def get_product_price(self):
        """ Get product actual product price and previous price """

        product_price = {
            'price': self.product.discount_price or self.product.price,
            'old_price': self.product.price if self.product.discount_price else None
        }
        role_price = self._get_price_by_user_role()
        if role_price:
            product_price = role_price
        return product_price

    def get_product_data(self):
        """ Get product data by serializer """
        serializer = ProductSerializer(self.product, fields=self.fields)
        return serializer.data

    def average_feedback_rating(self):
        """ Average feedback rating"""
        product_feedbacks = self.product.product_feedback.all()
        result = 0
        for item in product_feedbacks:
            if item.status == 'approved':
                result = result + item.rating
        if result:
            return result / len(product_feedbacks)

    def generate_product_data(self):
        """ Add additional data for product """
        product_data = self.get_product_data()
        if self.additional_fields:
            if 'variation_price' in self.additional_fields:
                product_data['variation_price'] = self.get_variation_price()
            if 'product_price' in self.additional_fields:
                product_data['product_price'] = self.get_product_price()
            if 'get_url' in self.additional_fields:
                product_data['get_url'] = self.get_url()
            if 'average_feedback_rating' in self.additional_fields:
                product_data['average_feedback_rating'] = self.average_feedback_rating()
            if 'seo_data' in self.additional_fields:
                seo_setting = self._get_seo_settings()
                product_data['seo_title'] = self._get_meta_title(seo_setting)
                product_data['seo_description'] = self._get_meta_description(seo_setting)
                product_data['seo_keywords'] = self.product.seo_keywords
                product_data['seo_nofollow'] = self.product.seo_nofollow
        return product_data

    def short_data(self, queryset):
        """Short data for list products"""
        serializer = ProductSerializer(queryset, many=True, fields=self.fields)
        return serializer.data

    def get_relations(self, count=3):
        """Get relation products"""
        result = list()
        get_relation_product = self.product.relation_products.all().order_by('?')[:int(count)]
        for item in get_relation_product:
            get_product_item = ProductData(product=item, fields=self.fields, additional_fields=self.additional_fields,
                                           user_role=self.user_role)
            result.append(get_product_item.generate_product_data())
        return result

    def get_same_products(self, count=3):
        """Get the same products"""
        queryset = Product.objects.prefetch_related('product_variation', 'category', 'category__menu_category_set',
                                                    'productrole_set').select_related(
            'measurement_id').filter(category__in=self.product.category.all()).exclude(
            slug=self.slug).all().order_by('?')[:int(count)]
        result = list()
        for item in queryset:
            get_product_item = ProductData(product=item, fields=self.fields, additional_fields=self.additional_fields,
                                           user_role=self.user_role)
            result.append(get_product_item.generate_product_data())
        return result
