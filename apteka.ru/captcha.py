import aiohttp
import asyncio
import time
import requests


async def captcha():
    put_url = 'https://err.apteka.ru/Captcha/ChooseFork?startX=649.5&startY=487.1953125&endX=863&endY=443.1953125'
    get_url = 'https://err.apteka.ru/Captcha/EyeDamaged'
    root_url = 'https://apteka.ru'
    addr_proxy = 'b8333g:0pwBvA@80.243.135.238:8000'
    proxy_adapter = {'http': f'http://{addr_proxy}', 'https': f'http://{addr_proxy}'}

    SITE_HEADERS_BEFORE_CAPTCHA = {
        'Host': 'apteka.ru',
        'Connection': 'keep-alive',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'Sec-Ch-Ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'Sec-Ch-Ua-Mobile': '?0',
        'Sec-Ch-Ua-Platform': '"Windows"',
        'Dnt': '1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-User': '?1',
        'Sec-Fetch-Dest': 'document',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ru-RU,ru;q=0.9',
    }

    HEAD = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Host': 'apteka.ru',
        'Pragma': 'no-cache',
        'Referer': 'https://err.apteka.ru/',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="119", "Chromium";v="119", "Not?A_Brand";v="24"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': "macOS",
    }

    cookie = {'X-Ciid-H': 'M5HGzljFyIIAAAAH5Xf3J', 'X-Ciid-B': 'KVV1yH3p8AnB3H1RfJxsvLlZwIhhbXHFAKytQ'}

    async with aiohttp.ClientSession(headers=HEAD, cookies=cookie) as session:
        async with session.get(root_url, proxy=proxy_adapter['http'], cookies=cookie, allow_redirects=False) as result:
            if result.status == 302:
                location = result.headers['Location']
            else:
                status = result.status
                return
        hed = SITE_HEADERS_BEFORE_CAPTCHA.copy()
        hed['Referer'] = location
        hed['Host'] = 'err.apteka.ru'

        async with session.put(put_url, proxy=proxy_adapter['http'], headers=hed, allow_redirects=False) as result:
            x_h = result.headers['X-Ciid-H']
        session.cookie_jar.update_cookies({'X-Ciid-H': x_h})
        hed['X-Ciid-H'] = x_h
        time.sleep(4)

        async with session.get(get_url, proxy=proxy_adapter['http'], headers=hed, allow_redirects=False) as result:
            x_b = result.headers['X-Ciid-B']
        session.cookie_jar.update_cookies({'X-Ciid-B': x_b})
        hed['Referer'] = 'https://err.apteka.ru/'
        hed['Host'] = 'apteka.ru'
        del hed['X-Ciid-H']
        async with session.get(root_url, proxy=proxy_adapter['http'], headers=hed, allow_redirects=False) as result:
            headers = result.headers
            status = result.status
            result1 = await result.text()
            result1 = 1


asyncio.run(captcha())
