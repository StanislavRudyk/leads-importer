from email_validator import validate_email ,EmailNotValidError 
import phonenumbers 
from phonenumbers import PhoneNumberFormat 
import pycountry 

def normalize_email (email :str )->str :
    try :
        email_info =validate_email (email ,check_deliverability =False )
    except EmailNotValidError :
        return None 
    return email_info .normalized 

def normalize_phone (phone :str )->str :
    if not phone :
        return None 
    phone_str =str (phone ).strip ()
    regions_to_guess =[None ,'GB','US','UA','PL','DE','AU','CA','AE']
    for region in regions_to_guess :
        try :
            parsed_phone =phonenumbers .parse (phone_str ,region )
            if phonenumbers .is_valid_number (parsed_phone ):
                return phonenumbers .format_number (parsed_phone ,PhoneNumberFormat .E164 )
        except phonenumbers .NumberParseException :
            continue 
    return None 

def normalize_country (country :str )->str :
    try :
        country_info =pycountry .countries .get (name =country )
        if not country_info :
            country_info =pycountry .countries .search_fuzzy (country )[0 ]
        return country_info .alpha_2 
    except (LookupError ,AttributeError ):
        return None 

def normalize_nationality (nationality_input :str )->str :
    code =normalize_country (nationality_input )
    mapping ={'UA':'Ukrainian','PL':'Polish','DE':'German','GB':'British','US':'American'}
    return mapping .get (code )

def normalize_city (city :str ,country :str )->str :
    if not city :
        return None 
    if not country :
        return city .strip ().title ()
    if city .strip ().lower ()==country .strip ().lower ():
        return None 
    return city .strip ().capitalize ()
