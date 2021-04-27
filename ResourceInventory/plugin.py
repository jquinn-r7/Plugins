from DivvyPlugins.plugin_metadata import PluginMetadata


class metadata(PluginMetadata):
    version = '1.0'
    last_updated_date = '2021-01-19'
    author = 'DivvyCloud Inc.'
    nickname = 'Resource Inventory APIs'
    default_language_description = (
        'Custom API endpoints to allow for inventory extraction'
    )
    support_email = 'support@divvycloud.com'
    support_url = 'http://support.divvycloud.com'
    main_url = 'http://www.divvycloud.com'
    managed = True


def load():
    pass


def unload():
    pass
