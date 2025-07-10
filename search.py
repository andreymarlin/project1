from flask import Flask, render_template, url_for, request, flash

# config
DATABASE = '/home/glushenko/Desktop/project1/data/1Mlines-RUSMARC-PubLib_utf-8.txt'
DEBUG = True
SECRET_KEY = 'jhdhkjdfhfkldgj1879fnkjhcvbjkddjkhdjvbv'

app = Flask(__name__)
app.config.from_object(__name__)

menu = [{"name": "Tuning", "url": "install-flask"},
        {"name": "First Api", "url": "first-app"},
        {"name": "Search", "url": "records-search"}]

@app.route("/")
def index():
    print(url_for('index'))
    return render_template("base.html", title='About page', menu = menu)

@app.route("/records-search", methods=["POST", "GET"])
def about():
    if request.method == 'POST':
        if len(request.form['query']) > 0:
            flash('Search is in process.', category='success')
        else:
            flash('Incorrect query.', category='error')
        dict1 = {'query':request.form['query']}
        print(dict1)
    return render_template("records-search.html", title='Search', menu = menu)

if __name__ == "__main__":
    app.run(debug=True, port=7288)