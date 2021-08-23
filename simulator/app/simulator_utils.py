import numpy as np

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


def timestep_to_dict(timestep, ctx):
    """
    Returns a dict representation of a Consumption_Timestep instance for serialization.
    Args:
        timestep (Consumption_Timestep): Consumption_Timestep instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with consumption timestep attributes to be serialized.
    """
    return dict(mw=timestep.mw,
                temperature=timestep.temperature,
                daytime=timestep.daytime)

def white_noise(rho, sr, n, mu=11):
    sigma = rho * np.sqrt(sr/2)
    noise = np.random.normal(mu, sigma, n)
    return noise
