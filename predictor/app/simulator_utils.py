

class Consumption_Timestep(object):
    """
    Timestep of energy consumption (15 min)
    Args:
        mw (float): Consumption in megawatt
        temperature (float): Temperature at the time of consumption
        daytime (float): Daytime of consumption as float (i.e. 0.15 for 00:15)
    """
    def __init__(self, mw, temperature, daytime):
        self.mw = mw
        self.temperature = temperature
        self.daytime = daytime


def timestep_to_dict(timestep):
    """
    Returns a dict representation of a Consumption_Timestep instance for serialization.
    Args:
        timestep (Consumption_Timestep): Consumption_Timestep instance.
    Returns:
        dict: Dict populated with consumption timestep attributes to be serialized.
    """
    return dict(mw=timestep.mw,
                temperature=timestep.temperature,
                daytime=timestep.daytime)

def dict_to_timestep(obj, ctx):
    """
    Converts object literal(dict) to a Consumption_Timestep instance.
    Args:
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
        obj (dict): Object literal(dict)
    """
    if obj is None:
        return None

    return Consumption_Timestep(mw=obj['mw'],
                temperature=obj['temperature'],
                daytime=obj['daytime'])
