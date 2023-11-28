from django.db import models

def create_monthly_model(month):
    class Meta:
        db_table = f'wiki_page_{month}_2023'
        indexes = [
            models.Index(fields=['keyword'], name=f'idx_keyword_{month}_2023')
        ]

    attrs = {
        '__module__': __name__,
        'Meta': Meta,
        'id': models.CharField(primary_key=True, max_length=128, default=''),
        'keyword': models.CharField(max_length=32, default=''),
        'views': models.CharField(max_length=1024),
        '__str__': lambda self: self.id,
    }

    model_name = f'Wikipage{month}2023'
    model = type(model_name, (models.Model,), attrs)
    return model

monthly_models = {f'{month:02}': create_monthly_model(month) for month in range(1, 13)}
