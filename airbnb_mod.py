import os
import re
import pandas as pd
from faker import Faker
import random
import gspread
from gspread_dataframe import set_with_dataframe
import datetime


def generate_data():
    fake = Faker()
    data_list = []
    # Set the seed for reproducibility
    # random.seed(42)

    # Define lists for the given options
    names = ["Black Lake Cabin", "Radcliff Refuge", "Rockaway Beach Villa", "The Red Bungalow of Deerfield",
             "The Retreat On Lansdale",
             "The Inn at Shorewood Township", "Chez Christopher’s", "Danielle’s Boutique Inn", "The Easy in Brooklyn",
             "The Tired Traveler Inn", "The Darling North Sanctuary!", "The Brooklyn Penthouse", "East Shore Hideaway",
             "Chinatown Artist Loft", "The Sunset", "The Iconic Allen Street Clubhouse", "The Little James st. Manor",
             "Williamsburg Abode",
             "The Home Away from Home", "Maui Beach Retreat", "The Home Sweet Home", "Palm Springs Bungalow Hideaway",
             "The Ruby Garden", "The Coral Reef Estate", "The Perfect Retreat", "Casa Ahora", "Sandy Beachfront Oasis",
             "The Northern Colonial",
             "Cozy Guesthouse", "West Shore Country Hale", "The Secret Westside Lounge", "The Little Peaceful Retreat",
             "Pennsylvania Avenue Best Kept Secret", "Silverlake Hills Hidden Gem", "Charming Retreat on the Eastside",
             "Hawthorne Crown Jewel", "The Magenta", "The Johnston’s Brownstone Inn", "Miami Getaway",
             "Cozy Cool Cabin",
             "The Bungalow at 7th Avenue", "The Oasis", "The Award Winning Peter Street B&B", "The Glen",
             "Downtown Penthouse Oasis", "Baker Avenue Bungalow", "The Starfish", "The Ultimate Escape",
             "The Artist’s Studio", "Rosie’s Retreat", "Tabglewoods", "Quinta Da Santana", "Redstone Villa",
             "Curly Coelho Cottage", "The Chalet", "Woodyvyu Stok House", "Singtom Resort", "Himalayan Home",
             "Junoon In The Hills", "La Belle Vie", "Romantic AC Studio", "Oasis of Trees and Tranquility",
             "The Jackfruit Tree",""]

    descriptions = ["Clean & quiet apt home by the park", "Skylit Midtown Castle",
                    "Entire Apt: Spacious Studio/Loft by central park", "Large Cozy 1 BR Apartment In Midtown East",
                    "BlissArtsSpace!", "Large Furnished Room Near B'way", "Cozy Clean Guest Room - Family Apt",
                    "Beautiful 1br on Upper West Side", "Cute apt in artist's home",
                    "Beautiful Sunny Park Slope Brooklyn", "West Side Retreat", "Modern Brooklyn Apt., August sublet",
                    "1,800 sq foot in luxury building", "Sunny 2-story Brooklyn townhouse w deck and garden"
                                                        "Times Square", " Safe, Clean and Cozy!",
                    "Cozy Room #3, Landmark Home 1 Block to PRATT", "ACCOMMODATIONS GALORE #1",
                    "Sunny Room in New Condo", "Stylish & Sleek Apartment Near SoHo!", "Sanctuary in East Flatbush",
                    "Cozy BR in Wiliamsburg 3 Bedroom", "Sunny Room in Old Historical Brooklyn Townhouse",
                    "Sun Filled Classic West Village Apt", "Lg Rm in Historic Prospect Heights",
                    "Financial District Luxury Loft", "BROOKLYN VICTORIAN STYLE SUITE.....",
                    "Your own Lovely West Village Studio", "ACCOMMODATIONS GALORE#3. 1-5 GUESTS",
                    "Greenwich Village Stylish Apartment", "Clean and Cozy Harlem Apartment",
                    "Fort Greene, Brooklyn: Center Bedroom", "Beautiful Queens Brownstone! - 5BR",
                    "Park Slope haven 15 mins from Soho", "Cozy 2 br in sunny Fort Greene apt",
                    "Cozy East Village Railroad 1 Bed!", "Great new apt, close to everything",
                    "Quiet & Clean Retreat in the City", "Stylish Large Gramercy Loft!",
                    "The Brownstone-Luxury 1 Bd Apt/NYC", "PRIVATE Room on Historic Sugar Hill", "South Slope Green",
                    "17 Flr. Manhattan Views, Nr. Subway", "Spacious 1BR, Adorable Clean Quiet",
                    "Nice, clean, safe, convenient 3BR", "Franklin St Flat in Trendy Greenpoint Brooklyn,",
                    "Artistic, Cozy, and Spacious w/ Patio! Sleeps 5",
                    "Huge Chelsea Loft", "Great room in great location", "Exclusive Room with Private Bath in  LES",
                    "Quiet, clean midtown apt w. elevato", "BROOKLYN > Guest Room w/ Queen Bed in Williamsburg",
                    "Share a HOME - $75 for 1 bdrm/++ for 2 - Brooklyn", "CHARMING EAST VILLAGE 2 (or 1) BR",
                    "Farmhouse Apartment in Williamsburg",
                    "Prewar Penthouse w Private Terrace", "Designer 2.5 BR Loft in Carroll Gardens by Subway",
                    "Bright Beautiful Brooklyn", "Nice renovated apt, prime location!",
                    "Private Bedroom in Large NYC Apartment", "Gigantic Private Brooklyn Loft!",
                    "Condo Apartment with laundry in unit", "1BR: See Central Park from Terrace!",
                    "Private room in cozy Greenpoint", "Food & Music Dream Apartment in Williamsburg",
                    "French Garden cottage off Bedford", "Luxury 3 bed/ 2 bath apt in Harlem w/ terrace",
                    "One Bedroom Mini studio - Free WIFI", "Eveland the Place to Stay & Enjoy a 5-⭐️ 2bdrm",
                    "Historic House Boerum Hill, BK, NYC",
                    "Lovely 3 bedroom in Italianate Brownstone w/garden", "MANHATTAN Neat, Nice, Bright ROOM",
                    "Colorful Artistic Williamsburg Apt", "UWS Brownstone Near Central Park",
                    "Artsy TopFloor Apt in PRIME BEDFORD Williamsburg", "2 BR w/ Terrace @ Box House Hotel",
                    "BOHEMIAN EAST VILLAGE 2 BED HAVEN", "Oceanfront Apartment in Rockaway",
                    "Private 1-Bedroom Apt in Townhouse", "Bright Room With A Great River View",
                    "Tree lined block modern apartment", "Sweet Historic Greenpoint Duplex",
                    "Riverside Charm with Fire Place", "Very Central, Nomad/Chelsea Loft Studio",
                    "Sunny 3BR Apt Ideal for Family", "Cozy Private Room in West Harlem!",
                    "UES Quiet & Spacious 1 bdrm for 4",
                    "ALL ABOUT A VERY COMFORTABLE ROOM..", "Brooklyn- Crown Heights Garden Apt.",
                    "Cozy room in Time Square!", "Large Park Slope Townhouse Duplex", "Lower East Side 2 Bedroom Apt",
                    "Manhattan Studio, Perfect Location", "Spacious Brooklyn Loft - 2 Bedroom",
                    "NYC Studio for Rent in Townhouse", "⚡Quiet Gem w/roof deck on NY's",
                    "Authentic New York City Living",
                    "SUPER BIG AND COZY PRIVATE BEDROOM", "☆Massive DUPLEX☆ 2BR & 2BTH East Village 9+ Guests",
                    "Gorgeous Upper West Side Apartment", "EAST VILLAGE STUDIO, sunny & quiet",
                    "Artsy 1 bedroom Apt. 20 min to 42nd Grand Central!", "Astoria-Private Home NYC-",
                    "BIG, COMFY , PRIV. ROOM, BIG APT, YARD, GREAT LOC.", "CHARMING PRIVATE BEDROOM EAST VILLAGE",
                    "City Room - Semi Private Bedroom", "Female Only Clean15min to Manhattan",
                    "Private E. Village Townhouse Stay", "Harlem/Hamilton Heights Cozy Room",
                    "Incredible Prime Williamsburg Loft!", "Entire Apt in Heart of Williamsburg",
                    "Clinton Hill + Free Coffee = #smile", "Chelsea living, 2BR best location",
                    "LOCATION LOCATION LOCATION UWS 60's",
                    "Sunny 15min to Manhattan LADY only", "Surfer room 15mins to downtown NYC!",
                    "Spacious Quiet rm - 20mins to Midtown", "Charming upper west side apartment",
                    "Sunny, quiet, legal homelike suite-Pk Slope South", "Elegant 2-BR duplex, Union Square",
                    "Williamsburg Home Away From Home!""Brand New Beautiful Duplex Apartment with Garden",
                    "Williamsburg bedroom by Bedford Ave",
                    "Clean and bright with a comfortable atmosphere", "Historic Brooklyn Studio Apartment",
                    "Room in Chic Modern High Line Luxury- New!", "Family & Friends in New York City",
                    "Spacious & Comfy BK Brownstone", "Large Luxury Upper East Side Studio",
                    "Chateau Style Brooklyn Loft for Singles or Couples", "Lovely Brooklyn Brownstone 1BR!",
                    "Sun-drenched East Village Penthouse",
                    "Columbus Circle Luxury Bldg - Private Room&Bath",
                    "Spacious, Kid-Friendly, and 15-20 Mins. to Midtown", "Quiet Chelsea Studio w/Charm",
                    "Large Room Overlooking Central Park", "Cheerful, comfortable room",
                    "Hospitality on Propsect Pk-12 yrs Hosting Legally!",
                    "Prospect Pk*NYC in 5 stops* Cozy,Clean & Legal!", "Loft Style Apt in Williamsburg",
                    "Comfy, Cozy, Brooklyn close to Manhattan",
                    "Sunny Space in Williamsburg", "Serene Park Slope Garden Apartment",
                    "Cozy and spacious - rare for NYC!", "Ideal Brooklyn Brownstone Apartment",
                    "Private Entrance - Private Parking", "Sunny, Spacious Studio in Ft.Greene",
                    "Williamsburg Exposed Brick Loft", "Bright Modern Charming Housebarge",
                    "Central Bedford Avenue Apartment",
                    "Welcome to Brooklyn! Bed-Stuy", "Sunnyside NYC/ AC room/ city views/ near Midtown",
                    "Greenpoint Waterfront Loft", "2-bedroom share in heart of Greenwich Village!",
                    "1 Bedroom Pre War apt", "Quiet One Bedroom in Park Slope", "CreaTive Live-In Artspace/Birdsnest",
                    "RARE Penthouse Oasis featured on DesignSponge", "New Clean Spacious Bed & Breakfast",
                    "Modern Unique Studio in NYC",
                    "Luxury Furnished 1 bedro, Bay Ridge", "Wonderful Studio In Brooklyn, NY!!!",
                    "BEAUTIFUL APARTMENT, GREAT LOCATION", "Great Bedroom in Downtown Manhattan",
                    "Beautiful Brownstone", "Oversized Studio in Park Slope",
                    "Apt with EmpireState view-Subway around the corner",
                    "Zen Yankee Stadium Pad 5 Minutes To Manhattan!", "Monthly Apartment Rental",
                    "rooms for rent in Queens with piano",
                    "Spacious Stunning Harlem Townhouse", "LES private apt, 1 bedroom & more",
                    "LOCATION LOCATION LOCATION Liz's", "Private Room in Fort Green BK close to city",
                    "Private Room Near Brooklyn Museum", "The Notorious B.N.B. { The Wallace }",
                    "Great Studio in W. Village - NYC!", "Spacious Townhome Apt in Brooklyn",
                    "Landmark 2 Bedroom West Village NYC",
                    "2 bedroom apt in charming brick townhouse", "Room in East Village with Private Entrance",
                    "Cozy&Clean in a great neighborhood", "Prime Williamsburg 1/BD New Condo",
                    "Private room w/ queen bed + rooftop", "Luxury NYC 2 Bedroom with Terrace",
                    "Bright unique designer loft in Soho", "SUNNY 1 Bedroom APT in Fort Greene - BROOKLYN",
                    "2 Bedroom Gem - Prime LES Location",
                    "BIG ROOM / DOWNTOWN  LOFT /", "Private Quiet Room in the BEST Location!! ❤",
                    "2 Bedrooms For Groups, GREAT LOCATION", "Fabulous Room in East Willamsburg!",
                    "Quirky, cozy, and fun in Bushwick!", "Snug & Private Bedroom only 30-35 min to Manhattan",
                    "Big room @ independent Eco-friendly aprtmnt.", "Bed in loft apartment east Williamsburg/bushwick",
                    "Visit the Big Apple! Mini-MOMA. Enjoy all of NYC!",
                    "Brand New Cozy Woodside Studio- Close to NYC",
                    "Spacious/very clean/professional service/ PK SLOPE", "Upper East Side 3 bed/3 bath Skyline view!",
                    "Gorgeous private bedroom in the 2 bd apt", "Nolita/Soho Duplex Apartment with Rooftop",
                    "Chambers Street Luxury One Bedroom", "Cozy Apt near JFK", "Harlem Hideaway Guest Room",
                    "Sunny Room by Prospect Park", "Lovely railroad apartment",
                    "The Executive Spacious Cozy Home on the  Block.", "Prince single room",
                    "Super clean & new 1 bedroom apartment", "Manhattan Luxury 1-BDRM w/ Terrace",
                    "Spacious Private Room in two bedroom apartment", "Cozy Studio in the Heart of Greenwich Village",
                    "Entire Apartment in Historic Brownstone + Garden",
                    "Serenity Falls in lovely Astoria 15min to the city",
                    "Spacious Brownstone Apt with Private Backyard",
                    "Cozy and Quiet", "Amazing room in Prime East Village!",
                    "Large, Open, Airy Room, Close to Subway Harlem NYC", "Comfortably Simple",
                    "Coliving in Brooklyn! Modern design / Shared room", "Café DuChill — now supporting ASPCA",
                    "☆Stylish Family + Group Friendly 3BR w/ Roof Patio", "Queens Artist' Corner",
                    "Huge Designer Room | Lower East Side /East Village",
                    "Cozy Apartment for Cat Lovers! (Kids friendly)",
                    "Pre-war apartment"
                    ]
    building_types = ["Entire house", "Apartment", "Private room", "Shared room"]
    property_type = ["Condominium", "Loft", "Townhouse","Cabin","Apartment", "House","Bed & Breakfast", "Unconventional"]
    host_names = ["Madaline", "Michelle", "Wilson", "Jenna", "Lyndon", "Emma", "Carl", "Alan", "Joyce", "Alina",
                  "Kevin", "Daniel",
                  "Victoria", "Anna", "Bruce", "Wilson", "Adele", "Brad", "Andrews", "Ross", "Rogers", "Martin",
                  "Murphy", "Clark",
                  "Owens", "Tucker", "Adams", "Thomas", "Harris", "Robinson", "Reed", "Carroll", "Wright", "Phillips",
                  "Montgomery",
                  "Johnson", "Lloyd", "Miller", "Cole", "Wells", "Mitchell", "Holmes", "Cameron", "Marcus", "Walter",
                  "Sawyer",
                  "Kevin", "Albert", "Sarah", "Henry", "Cavil", "Adrian", "Shepard", "Carlos", "Victoria", "Gianna",
                  "Jones",
                  "Fedrick", "Davis", "John", "Armstrong", "Hunt", "Lloyd", "Hawkins", "Hill", "Spencer", "Perkins",
                  "Davis",
                  "Ferguson", "Anderson", "Payne", "Miller", "Reed", "Stewart", "Grant", "Perry", "Andrews", "Gibson",
                  "Hill",
                  "Terrance", "Joe", "Suzanne", "Robert", "Andrew", "Jennifer", "Laetitia", "Serge", "Seth", "Ralph",
                  "Marc",
                  "Haley", "Joris", "Rachel", "Michelle", "James", "Danielle", "Jsun", "Diego", "Jay", "Muneeba", "Jay",
                  "Jay",
                  "Luisa", "Zel", "Alex", "Crystal", "David", "Shu", "Freddy", "Mark", "Diana And Peter", "Yukako",
                  "Matilda",
                  "Sam", "Marianne", "Layla", "Pirin", "Kazuya", "Anthony", "Georgette'S", "Zach", "Aiden & Cassy",
                  "Sara",
                  "Mariia", "Jimmy", "Vanessa", "Crystal", "Zach", "Studioplus", "Zach", "Yao & Rain", "Maria", "Sam",
                  "Camilla",
                  "Clara", "Karen Jennifer & Jan", "Jessica", "Norman", "Maira", "Lauren", "Stephanie", "Bill",
                  "Allan C.",
                  "Tyler", "Dany", "Nekhena", "Edward", "Baptiste", "Jim", "Mila", "Nikita", "Goenuel", "Elizabeth",
                  "Lionel",
                  "Kafayat", "Laura", "Liza"]

    neighbourhoods = ["Richmond in Melbourne", "Constitución in Argentina",
                      "The Bukit Peninsula in  Indonesia",
                      "District VII in  Hungary", "Poncey-Highland in  Georgia",
                      "Oak Lawn in  Texas",
                      "Roma Sur in MexicoCity", "Meireles in Brazil", "Hammerbrook in Germany",
                      "Triana in Spain", "Capucins in France"]

    states = ["Alabama","Florida","Georgia","Hawaii","Kansas","Kentucky","Maine","Michigan","Montana","Oregon", "California", "Arizona", "Alaska", "Dallas", "Arkansas", "Colorado", "London", "Germany", "Sydney",
              "Goa", "Delhi", "Mumbai", "Rishikesh", "Pune", "Norway","Bangalore","North Dakota","Oregon","Gujarat","Kolkata", "Poland", "Sweden", "Austria", "Greece",
              "Brisbane", "Perth", "Queensland", "Darwin", "Melbourne"]

    instant_bookables = ["Yes", "No"]
    cancellation_policy = ["Strict", "Moderate", "Flexible"]
    status = ["confirmed", "not confirmed"]
    host_status = ["confirmed", "not confirmed"]

    host_experience = ["Experienced", "Moderately Experienced", "Inexperienced"]
    # Define a list of fixed amenities
    fixed_amenities = ["Wifi", "Hot water bath", "Fully equipped kitchen", "proper double bed","first aid kits"]
    sea = ["Holiday Season","Non Holiday Season"]
    # Generate two random additional amenities from the provided list
    additional_amenities = [
        "Air conditioning",
        "Pool",
        "Free parking",
        "Iron and iron board",
        "Washer and Dryer",
        "Smart locks for self check-in",
        "LED TV",
        "Coffee machine",
        "Heaters",
    ]

    fixed_food = ["Cereals", "Fruits", "Vegetables"]
    additional_food = [
        "Yogurt", "Milk", "OJ", "Eggs", "Juices", "Muffins", "Bread and Jam",
    ]

    # Generate data
    for _ in range(2000):  # You can change the number of data points as needed
        val_status = random.choice(status)
        host_test = random.choice(host_status)
        neighborhood = random.choice(neighbourhoods)
        host_exp = random.choice(host_experience)

        # Extract state name after "in" in the neighborhood
        state_match = re.search(r'in (\w+)', neighborhood, re.IGNORECASE)
        if state_match:
            state = state_match.group(1)
        else:
            state = random.choice(states)

        # Generate random construction and accommodation years
        start_date = datetime.date(2008, 1, 1)
        end_date = datetime.date(2022, 12, 31)

        # Generate a random date between start_date and end_date
        construction_year = fake.date_between(start_date=start_date, end_date=end_date)

        # Calculate the accommodation date as one year ahead of the construction date
        try:
            accommodation_year = construction_year.replace(year=construction_year.year + 1)
        except ValueError:
            # Handle invalid dates as needed
            pass
        # Ensure accommodation_year > construction_year
        random_additional_amenities = random.sample(additional_amenities, 3)
        random_additional_food = random.sample(additional_food, 2)
        
        Booking_total = round(random.uniform(64, 90), 2)
        Host_fee = 0 if host_test == "not confirmed" else round(random.uniform(0.11, 0.16), 2)
        Reviews_per_month = round(random.uniform(1, 10), 2)
        Review_rate_number = fake.random_int(min=1, max=10)
        Food_cost = round(random.uniform(10, 60), 2)
        Service_fee = 0 if val_status == "not confirmed" else round(random.uniform(0.2, 0.8), 2)
        # Add amenities to the data dictionary

        data = {
            "id": fake.random_int(min=1000000, max=9999999),
            "Name": random.choice(names),
            "Description": random.choice(descriptions),
            "Building type": random.choice(building_types),
            "Property type":random.choice(property_type),
            "Amenities": fixed_amenities + random_additional_amenities,
            "host_id": fake.random_int(min=1000000, max=9999999),
            "Host_name": random.choice(host_names),
            "Host_identity_verified": host_test,
            "Host_Experience": host_exp,
            "Host_fee": Host_fee,
            "Neighbourhood": neighborhood,
            "State": state,
            "Zipcode": fake.random_int(min=1000, max=9999),
            "Currency": "usd",
            "Season":random.choice(sea),
            "Instant_bookable": random.choice(instant_bookables),
            "Construction_year": construction_year,
            "Accommodation_year": accommodation_year,
            "Commission": round(random.uniform(0.10, 0.15), 2),
            "Booking_total": Booking_total,
            "Status": val_status,
            "Bookings": fake.random_int(min=1, max=1000),

            "Service_fee": Service_fee,
            "Cancellation_Policy":random.choice(cancellation_policy),
            "Food_cost": Food_cost,
            "Food_type": fixed_food + random_additional_food,
            "Minimum_nights": fake.random_int(min=1, max=500),
            "Reviews": random.choice([
                "When we first stumbled upon this flat on AirBnB, it seemed almost too good to be true. There must be a catch! But everything was as perfect as it seemed online. (Host Name) is the most thoughtful, gracious host.",
                "It would be my pleasure to host them again anytime. They left the place so spotless; I could not even tell if they stayed in the home! Thanks so much! I sincerely hope to host you again",
                "They were easy to communicate with and left the space in excellent condition. We would be happy to host them again."
            ]),
            "Reviews_per_month": Reviews_per_month,
            "Review_rate_number": Review_rate_number,
            "House_rules": random.choice([
                "Clean up and treat the home the way you'd like your home to be treated",
                "No smoking",
                "Pet friendly but please confirm with me if the pet you are planning on bringing with you is OK",
                " I have a cute and quiet mixed chihuahua.",
                "I could accept more guests (for an extra fee) but this also needs to be confirmed beforehand.",
                "Also friends traveling together could sleep in separate beds for an extra fee (the second bed is either a sofa bed or inflatable bed). Smoking is only allowed on the porch.",
                "My ideal guests would be warm, friendly, and respectful of sharing my home and its rhythms.",
                " I am allergic to cigarettes, so no smoking please, not even in the yard. A quiet homecoming is much appreciated at the end of the evening's nightlife."
            ]),
            "Host_Revenue": (Booking_total*Host_fee)+(0.1*Booking_total)+15,
            "Platform_Revenue": 0.3*Reviews_per_month*0.9*Review_rate_number,
            "Total_Revenue": (0.2 * Booking_total + Food_cost + 15) + (0.3*Reviews_per_month*0.9*Review_rate_number) if host_exp == "experienced" else (0.03 * Booking_total + Service_fee * Booking_total + Food_cost + 15) + (0.3*Reviews_per_month*0.9*Review_rate_number)
        }
        # print(data)
        data_list.append(data)

    df = pd.DataFrame(data_list)
    #temp_dir = tempfile.mkdtemp()
    #df_path = temp_dir + "/df.csv"
    df.to_csv("Airbnbfeeder.csv", index=False)
    df.to_excel("Airbnbfeeder.xlsx", index=False)
    df.to_json("AirbnbApi.json", orient="records")
    # Print the DataFrame
    return df


def write_df(**kwargs):
    #ti = kwargs["task_instance"]
    #df = ti.xcom_pull(task_ids="generate_data_task")
    
    df = generate_data()
    GSHEET_NAME = 'AirbnbFeeder'
    TAB_NAME = 'Content'
    credentialsPath = os.path.expanduser("credentials\\diamond-analysis-ac6758ca1ace.json")

    if os.path.isfile(credentialsPath):
        # Authenticate and open the Google Sheet
        gc = gspread.service_account(filename=credentialsPath)
        sh = gc.open(GSHEET_NAME)
        worksheet = sh.worksheet(TAB_NAME)

        set_with_dataframe(worksheet, df)

        # Now, 'df' contains the data from the Google Sheet
        print("Data loaded successfully!! Have fun!!")
        print(df)
    else:
        print(f"Credentials file not found at {credentialsPath}")


write_df()
