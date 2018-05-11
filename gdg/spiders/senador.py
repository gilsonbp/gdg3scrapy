import scrapy
import json


class SenadorItem(scrapy.Item):
    """
    Items com os campos que serão importados
    """
    nome = scrapy.Field()
    data_nascimento = scrapy.Field()
    telefones = scrapy.Field()
    email = scrapy.Field()
    url_perfil = scrapy.Field()


class SenadorSpider(scrapy.Spider):
    """
   Comando para importar somente os campos do Item para um arquivo CSV
   scrapy crawl senador -t csv -o senadores.csv --loglevel=INFO

    """

    name = 'senador'

    def start_requests(self):
        yield scrapy.Request(
            url='http://legis.senado.gov.br/dadosabertos/senador/lista/atual',
            headers={'Accept': 'application/json'}
        )

    def parse(self, response):
        jsonresponse = json.loads(response.body_as_unicode())
        for pl in jsonresponse['ListaParlamentarEmExercicio']['Parlamentares']['Parlamentar']:
            idp = pl.get('IdentificacaoParlamentar', False)
            if idp:
                yield scrapy.Request(
                    url=idp['UrlPaginaParlamentar'],
                    callback=self.parse_perfil
                )

    def parse_perfil(self, response):
        dados_pessoais = response.xpath('//div[contains(@class, "dadosPessoais")]')

        fax = dados_pessoais.xpath('./dl/dt[6]/text()').extract_first()
        if fax == 'FAX:':
            email = dados_pessoais.xpath('./dl/dd[7]/a/text()').extract_first()
        else:
            email = dados_pessoais.xpath('./dl/dd[6]/a/text()').extract_first()

        item = SenadorItem()
        item['nome'] = dados_pessoais.xpath('./dl/dd[1]/text()').extract_first()
        item['data_nascimento'] = dados_pessoais.xpath('./dl/dd[2]/text()').extract_first()
        item['telefones'] = dados_pessoais.xpath('./dl/dd[5]/text()').extract_first()
        item['email'] = email
        item['url_perfil'] = response.url
        yield item
