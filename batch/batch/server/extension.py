from datetime import date, datetime
import re
import kubernetes as kube
from kubernetes.client import models
from six import PY3, integer_types, iteritems, text_type

v1 = kube.client.CoreV1Api()

def deserialize(api_client, data, klass):
    """
    Deserializes dict, list, str into an object.

    :param data: dict, list or str.
    :param klass: class literal, or string of class name.

    :return: object.
    """

    if data is None:
        return None

    if type(klass) == str:
        if klass.startswith('list['):
            sub_kls = re.match('list\[(.*)\]', klass).group(1)
            return [deserialize(api_client,sub_data, sub_kls)
                    for sub_data in data]

        if klass.startswith('dict('):
            sub_kls = re.match('dict\(([^,]*), (.*)\)', klass).group(2)
            return {k: deserialize(api_client, v, sub_kls)
                    for k, v in iteritems(data)}

        # convert str to class
        if klass in api_client.NATIVE_TYPES_MAPPING:
            klass = api_client.NATIVE_TYPES_MAPPING[klass]
        else:
            klass = getattr(models, klass)


    if klass in api_client.PRIMITIVE_TYPES:
        return deserialize_primitive(api_client, data, klass)
    elif klass == object:
        return deserialize_object(api_client, data)
    # elif klass == date:
    #     return deserialize_date(api_client, data)
    # elif klass == datetime:
    #     return deserialize_datatime(api_client, data)
    else:
        return deserialize_model(api_client, data, klass)

def deserialize_primitive(self, data, klass):
    """
    Deserializes string to primitive type.

    :param data: str.
    :param klass: class literal.

    :return: int, long, float, str, bool.wha
    """
    try:
        return klass(data)
    except UnicodeEncodeError:
        return unicode(data)
    except TypeError:
        return data

def deserialize_object(self, value):
    """
    Return a original value.

    :return: object.
    """
    return value


def deserialize_model(self, data, klass):
    """
    Deserializes list or dict to model.
    :param data: dict, list.
    :param klass: class literal.
    :return: model object.
    """

    if not klass.swagger_types and not hasattr(klass, 'get_real_child_model'):
        return data

    kwargs = {}
    if klass.swagger_types is not None:
        for attr, attr_type in iteritems(klass.swagger_types):
            if data is not None \
                    and attr in data \
                    and isinstance(data, (list, dict)):
                value = data[attr]
                kwargs[attr] = deserialize(self, value, attr_type)

    instance = klass(**kwargs)

    if hasattr(instance, 'get_real_child_model'):
        klass_name = instance.get_real_child_model(data)
        if klass_name:
            instance = deserialize(self, data, klass_name)
    return instance