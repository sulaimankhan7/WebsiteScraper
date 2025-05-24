class SiteData:
    def __init__(self):
        pass


class PageData:
    def __init__(self, url: str, content: str, text: str, images: list):
        self.url = url
        self.content = content
        self.text = text
        self.images = images

    def __str__(self):
        return self.url

    __repr__ = __str__