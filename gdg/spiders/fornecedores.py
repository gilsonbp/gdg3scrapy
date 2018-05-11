import scrapy
import json


class FornecedorItem(scrapy.Item):
    """
    Items com os campos que serão importados
    """
    id = scrapy.Field()
    cnpj = scrapy.Field()
    razao_social = scrapy.Field()
    cep = scrapy.Field()


class FornecedoresSpider(scrapy.Spider):
    """
    Comando para importar somente os campos do Item para um arquivo CSV
    scrapy crawl fornecedores -t csv -o fornecedores.csv --loglevel=INFO
    """
    name = 'fornecedores'
    start_urls = ['http://compras.dados.gov.br/fornecedores/v1/fornecedores.json?uf=AL']
    url_raiz = 'http://compras.dados.gov.br'

    def parse(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        for field in jsonresponse['_embedded']['fornecedores']:
            if field.get('cnpj', None) is not None:
                url_fornecedor = self.url_raiz + '/fornecedores/doc/fornecedor_pj/' + field['cnpj'] + '.json'
                yield scrapy.Request(
                    url=url_fornecedor,
                    callback=self.parse_fornecedores_pj
                )

        _links = jsonresponse['_links']
        next_page = _links.get('next', False)
        self.log(next_page)
        if next_page:
            self.log(next_page)
            yield scrapy.Request(
                url=self.url_raiz + next_page['href'], callback=self.parse
            )

    def parse_fornecedores_pj(self, response):
        field = json.loads(response.body_as_unicode())

        item = FornecedorItem()
        item['id'] = field['id']
        item['cnpj'] = field['cnpj']
        item['razao_social'] = field['razao_social']
        item['cep'] = field.get('cep', '-')
        yield item
