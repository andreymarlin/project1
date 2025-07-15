import os
import sqlite3

from flask import Flask, render_template, url_for, request, flash, g, jsonify
from FDataBase import FDataBase

# config
DATABASE = "/home/glushenko/Desktop/project1/project1/flsite.db"
DEBUG = True
SECRET_KEY = 'jhdhkjdfhfkldgj1879fnkjhcvbjkddjkhdjvbv'

app = Flask(__name__)
app.config.from_object(__name__)

app.config.update(dict(DATABASE=os.path.join(app.root_path,'flsite.db')))

def connect_db():
    conn = sqlite3.connect(app.config['DATABASE'])
    conn.row_factory = sqlite3.Row
    return conn

def create_db():
    db = connect_db()
    with app.open_resource('sq_db.sql', mode='r') as f:
        db.cursor().execute_script(f.read())
    db.commit()
    db.close()

def get_db():
    '''DB connection if it is not connected'''
    if not hasattr(g, 'link_db'):
        g.link_db = connect_db()
        return g.link_db
    
@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'link_db'):
        g.link_db.close()

menu = [{"name": "Checking readiness", "url": "ready"},
        {"name": "Search", "url": "search"},
        {"name": "Search-demo", "url": "records-search"}]

@app.route("/")
def index():
    # print(url_for('index'))
    return render_template("base.html", title='About page', menu = menu)

@app.route("/records-search", methods=["POST", "GET"])
def about():
    if request.method == 'POST':
        if len(request.form['query']) > 0:
            flash('Search is in process.', category='success')
            dict1 = {'query':request.form['query']}
            print(dict1)
            db = get_db()
            dbase = FDataBase(db)
            list_results = dbase.find_query(request.form['query'])
            print(list_results)
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