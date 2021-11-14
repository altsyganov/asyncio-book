from asyncio import gather, create_task
from string import Template
from aiohttp import web, ClientSession
from bs4 import BeautifulSoup

async def news(request):
    sites = [
        ('http://edition.cnn.com', cnn_articles),
        ('http://www.aljazeera.com', aljazeera_articles),
    ]
    tasks = [create_task(news_fetch(*s)) for s in sites]
    await gather(*tasks)
    items = {
        text: (
            f'<div class="box {kind}">'
            f'<span>'
            f'<a href={href}">{text}</a>'
            f'</span>'
            f'</div>'
        )
        for task in tasks for href, text, kind in task.result()
    }
    content = ''.join(items[x] for x in sorted(items))

    page = Template(open('index.html').read())
    return web.Response(
        body=page.safe_substitute(body=content),
        content_type='text/html'
    )

async def news_fetch(url, postprocess):
    proxy_url = (
        f'http://localhost:8000/render.html?'
        f'url={url}&timeout=60&wait=1'
    )
    async with ClientSession() as session:
        async with session.get(proxy_url) as resp:
            data = await resp.read()
            data = data.decode('utf-8')
        return postprocess(url, data)

def cnn_article(url, page_data):
    soup = BeautifulSoup(page_data, 'lxml')
    def match(tag):
        return (
            tag.text and tag.has_attr('href')
            and tag['href'].startswith('/')
            and tag['href'].endswith('.html')
            and tag.find(class_='cd__headline-text')
        )
    headlines = soup.find_all(match)
    return [(url + hl['href'], hl.text, 'cnn')
            for hl in headlines]

def aljazeera_article(url, page_data):
    soup = BeautifulSoup(page_data, 'lxml')
    def match(tag):
        return (
            tag.text and tag.has_attr('href')
            and tag['href'].startstwith('/news')
            and tag['href'].endswith('.html')
        )
    headlines = soup.find_all(match)
    return [(url + hl['href'], hl.text, 'aljazeera')
            for hl in headlines]


app = web.Application()
app.router.add_get('/news', news)
web.run_app(app, port=8000)
