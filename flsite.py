from flask import Flask, render_template, url_for

app = Flask(__name__)

menu = ["Tuning", "First Api", "Feedback"]

@app.route("/")
def index():
    print(url_for('index'))
    return render_template("index.html", title='About page', menu=menu)


@app.route("/profile/<username>")
def profile(username):
    return(f"User: (username)")

@app.route("/about")
def about():
    print(url_for('about'))
    return "<h1>About page</h1>"

if __name__ == "__main__":
    app.run(debug=True, port=7289)
