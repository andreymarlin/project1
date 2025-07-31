import re
from flask import Flask, render_template, request, flash, g, jsonify
from searchdb import search_query, search_author, search_query_author

FILE_PATH = '/media/storage/glushenko/Desktop/project1/data/AKO - введенные.TXT'

resp_dict = {}
resp_dict['k1'] = '\n#22: '
resp_dict['k8'] = '\n'
resp_dict['k4'] = ' ('
resp_dict['k2'] = ')\n'
resp_dict['k3'] = ' '
resp_dict['k5'] = '\n#621: '
resp_dict['k6'] = '\n#675: '
resp_dict['k7'] = '\n#686: '
resp_dict['k9'] = '\n#822: '
resp_dict['k10'] = '\n#852: '
resp_dict['k11'] = '\n#899: '
resp_dict['k12'] = '\n#903: '
resp_dict['k13'] = '\n#952: '
resp_dict['k14'] = '\n#2147483647: '

def parsing():
    # patterns to clean the data:
    pattern1 = r'\$[a-z]'
    pattern2 = r'\^[A-Z]'
    pattern3 = r'\^[a-z]'
    pattern4 = r'\^[А-Я]'

    """
    recs is a list of dictionaries.
    Each recs[i] is a dictionary which is corresponded to the record with an index i.
    Each dictionary contains pairs key-value with the following relations:
    'k0' - card text for embedding computation
    'k1' - field #22
    'k2' - field #200
    'k3' - field #210
    'k4' - field #328
    'k5' - field #621
    'k6' - field #675
    'k7' - field #686
    'k8' - field #700
    'k9' - field #822
    'k10' - field #852
    'k11' - field #899
    'k12' - field #903
    'k13' - field #952
    'k14' - field #2147483647
    'k15' - field 46
    'k16' - field 461
    'k17' - field 961
    """
    recs = []
    dictionary = {}
    text = []
    field22 = []
    field200 = []
    field210 = []
    field328 = []
    field621 = []
    field675 = []
    field686 = []
    field700 = []
    field822 = []
    field852 = []
    field899 = []
    field903 = []
    field952 = []
    field46 = []
    field461 = []
    field961 = []
    field_jpg = []
    with open(FILE_PATH, 'r', encoding='Windows-1251', errors='ignore') as f:
        for line in f:
            stripped_line = line.strip()
            if stripped_line:
                # If the separator ***** was found, then record is complete
                if stripped_line == '*****':
                    dictionary['k0'] = ' '.join(text).strip()
                    # if len(text) == 0:
                    #     dictionary['k0'] = ' '.join(field22).strip()
                    # else:
                    #     dictionary['k0'] = ' '.join(text).strip()
                    if len(field22) > 0:
                        dictionary['k1'] = ' '.join(field22).strip()
                    if len(field700) > 0:
                        dictionary['k8'] = ' '.join(field700).strip()
                    if len(field328) > 0:
                        dictionary['k4'] = ' '.join(field328).strip()
                    if len(field200) > 0:
                        dictionary['k2'] = ' '.join(field200).strip()
                    if len(field210) > 0:
                        dictionary['k3'] = ' '.join(field210).strip()
                    if len(field621) > 0:
                        dictionary['k5'] = ' '.join(field621).strip()
                    if len(field675) > 0:
                        dictionary['k6'] = ' '.join(field675).strip()
                    if len(field686) > 0:
                        dictionary['k7'] = ' '.join(field686).strip()
                    if len(field822) > 0:
                        dictionary['k9'] = ' '.join(field822).strip()
                    if len(field852) > 0:
                        dictionary['k10'] = ' '.join(field852).strip()
                    if len(field899) > 0:
                        dictionary['k11'] = ' '.join(field899).strip()
                    if len(field903) > 0:
                        dictionary['k12'] = ' '.join(field903).strip()
                    if len(field952) > 0:
                        dictionary['k13'] = ' '.join(field952).strip()
                    if len(field_jpg) > 0:
                        dictionary['k14'] = ' '.join(field_jpg).strip()
                    if len(field46) > 0:
                        dictionary['k15'] = ' '.join(field46).strip()
                    if len(field461) > 0:
                        dictionary['k16'] = ' '.join(field461).strip()
                    if len(field961) > 0:
                        dictionary['k17'] = ' '.join(field961).strip()
                    recs.append(dictionary)
                    dictionary = {}
                    text = []
                    field22 = []
                    field200 = []
                    field210 = []
                    field328 = []
                    field621 = []
                    field675 = []
                    field686 = []
                    field700 = []
                    field822 = []
                    field852 = []
                    field899 = []
                    field903 = []
                    field952 = []
                    field_jpg = []
                else:
                    str1 = re.sub(pattern1, ' ', stripped_line)
                    str2 = re.sub(pattern2, ' ', str1)
                    str3 = re.sub(pattern3, ' ', str2)
                    str4 = re.sub(pattern4, ' ', str3)
                    if "#22:" in str4:
                        field22.append(str4.rstrip(' ').replace("#22: ", ""))
                    if "#961:" in str4:
                        field961.append(str4.rstrip(' ').replace("#961: ", ""))
                        text.append(str4.rstrip(' ').replace("#961: ", ""))
                    if "#461:" in str4:
                        field46.append(str4.rstrip(' ').replace("#461: ", ""))
                        text.append(str4.rstrip(' ').replace("#461: ", ""))
                    if "#46:" in str4:
                        field461.append(str4.rstrip(' ').replace("#46: ", ""))
                        text.append(str4.rstrip(' ').replace("#46: ", ""))
                    if "#200:" in str4:
                        field200.append(str4.rstrip(' ').replace("#200: ", ""))
                        text.append(str4.rstrip(' ').replace("#200: ", ""))
                    if "#210:" in str4:
                        field210.append(str4.rstrip(' ').replace("#210: ", ""))
                        text.append(str4.rstrip(' ').replace("#210: ", ""))
                    if "#328:" in str4:
                        field328.append(str4.rstrip(' ').replace("#328: ", ""))
                        text.append(str4.rstrip(' ').replace("#328: ", ""))
                    if "#621:" in str4:
                        field621.append(str4.rstrip(' ').replace("#621: ", ""))
                    if "#675:" in str4:
                        field675.append(str4.rstrip(' ').replace("#675: ", ""))
                    if "#686:" in str4:
                        field686.append(str4.rstrip(' ').replace("#686: ", ""))
                    if "#700:" in str4:
                        field700.append(str4.rstrip(' ').replace("#700: ", ""))
                        text.append(str4.rstrip(' ').replace("#700: ", ""))
                    if "#822:" in str4:
                        field822.append(str4.rstrip(' ').replace("#822: ", ""))
                    if "#852:" in str4:
                        field852.append(str4.rstrip(' ').replace("#852: ", ""))
                    if "#899:" in str4:
                        field899.append(str4.rstrip(' ').replace("#899: ", ""))
                    if "#903:" in str4:
                        field903.append(str4.rstrip(' ').replace("#903: ", ""))
                    if "#952:" in str4:
                        field952.append(str4.rstrip(' ').replace("#952: ", ""))
                    if "#2147483547:" in str4:
                        field_jpg.append(str4.rstrip(' ').replace("#2147483547: ", ""))                  
        return recs

records = parsing()

# config
DATABASE = "/home/glushenko/Desktop/project1/project1/flsite.db"
DEBUG = True
SECRET_KEY = 'jhdhkjdfhfkldgj1879fnkjhcvbjkddjkhdjvbv'

app = Flask(__name__)
app.config.from_object(__name__)

menu = [{"name": "Checking readiness", "url": "ready"},
        {"name": "Search", "url": "search"},
        {"name": "Search-demo", "url": "records-search"}]

@app.route("/")
def index():
    return render_template("base.html", title='Библиотека ГПНТБ', menu = menu)

@app.route("/records-search", methods=["POST", "GET"])
def about():
    if request.method == 'POST':
        if len(request.form['query']) > 0 and len(request.form['author']) > 0:
            flash('Search is in process.', category='success')
            dict1 = {'query':request.form['query'], 'author':request.form['author']}
            search_results = search_query_author(dict1['query'], dict1['author'])

            if search_results != False:
                for id in search_results:
                    card = records[int(id)]
                    keys = resp_dict.keys()
                    output = ''
                    for key in keys:
                        if key in card:
                            output += resp_dict[key]
                            output += card[key]
                    flash(output)
                    flash('\n')
            else:
                flash('Nothing has been found.')

        elif len(request.form['query']) > 0 and len(request.form['author']) == 0:
            flash('Search is in process.', category='success')
            dict1 = {'query':request.form['query']}
            search_results = search_query(dict1['query'])

            if search_results != False:
                for id in search_results:
                    card = records[int(id)]
                    keys = resp_dict.keys()
                    output = ''
                    for key in keys:
                        if key in card:
                            output += resp_dict[key]
                            output += card[key]
                    flash(output)
                    flash('\n')
            else:
                flash('Nothing has been found.')

        elif len(request.form['query']) == 0 and len(request.form['author']) > 0:
            flash('Search is in process.', category='success')
            dict1 = {'author':request.form['author']}
            search_results = search_author(dict1['author'])
            
            if search_results != False:
                for id in search_results:
                    card = records[int(id)]
                    keys = resp_dict.keys()
                    output = ''
                    for key in keys:
                        if key in card:
                            output += resp_dict[key]
                            output += card[key]
                    flash(output)
                    flash('\n')
            else:
                flash('Nothing has been found.')
            
            
        else:
            flash('Incorrect query.', category='error')
    return render_template("records-search.html", title='Search', menu = menu)

@app.route('/ready', methods=['GET'])
def ready():
    '''
    Checks if the service is runnng.
    Returns "OK" and status 200 if the service is available.
    If the service is not running, it will not respond to the request.
    '''
    return 'OK', 200

@app.route('/search', methods=['POST'])
def search():
    '''
    Takes a JSON {"query":"text query"} and returns the search results.
    '''
    if not request.is_json:
        return jsonify({"error": "Content-Type must be application/json"}), 400
    
    data = request.get_json()

    if not data or 'query' not in data:
        return jsonify({"error": "JSON payload must contain a 'query' field."}), 400
    
    query = data['query']
    query_lower = query.lower()


    return jsonify({
        "query": query_lower,
        "results": 'results'
    })

if __name__ == "__main__":
    app.run(debug=True, port=7288)