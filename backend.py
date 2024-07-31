from flask import Flask, render_template, request, redirect, url_for, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask_sqlalchemy import SQLAlchemy
from flask_bcrypt import Bcrypt
from flask_migrate import Migrate
import re
import logging

app = Flask(__name__)

app.config['SECRET_KEY'] = 'ePYHc~dS*)8$+V-\'qzRtC{6rXN3NRgL'
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root1:Rootuser!123@localhost/sip_database'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['TEMPLATES_AUTO_RELOAD'] = True

db = SQLAlchemy(app)
bcrypt = Bcrypt(app)
migrate = Migrate(app, db)
logging.basicConfig(level=logging.DEBUG) 

class User(db.Model, UserMixin):
    __tablename__ = 'users'
    user_id = db.Column(db.Integer, primary_key=True,autoincrement=True, nullable = False)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    email = db.Column(db.String(100), unique=True, nullable=False)
    created_at = db.Column(db.DateTime, default=db.func.current_timestamp())

    def get_id(self):
        return (self.user_id)

    def set_password(self, password):
        self.password_hash = bcrypt.generate_password_hash(password).decode('utf-8')

    def verify_password(self, password):
        return bcrypt.check_password_hash(self.password_hash, password)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

@app.route("/login", methods=["GET", "POST"])
def login():
    msg = ''
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        user = User.query.filter_by(username=username).first()

        if user and user.verify_password(password):
            login_user(user, remember=request.form.get('remember'))
            return redirect(url_for('home'))
        else:
            flash('Invalid username or password')
    return render_template('user/login.html')

@app.route("/register", methods=["GET", "POST"])
def register():
    msg = ''
    if request.method == 'POST':
        # Fetch form data safely
        username = request.form.get('username')
        password = request.form.get('password')
        email = request.form.get('email')

        if not username or not password or not email:
            flash('Please fill out the form completely!')
        elif User.query.filter_by(username=username).first():
            flash('Account already exists!')
        elif not re.match(r'[^@]+@[^@]+\.[^@]+', email):
            flash('Invalid email address!')
        elif not re.match(r'[A-Za-z0-9]+', username):
            flash('Username must contain only characters and numbers!')
        else:
            new_user = User(username=username, email=email)
            new_user.set_password(password)
            db.session.add(new_user)
            db.session.commit()
            msg = 'You have successfully registered!'
    return render_template('user/register.html', msg=msg)

@app.route("/logout",methods =["POST"])
@login_required 
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route("/home")
@login_required
def home():
    return render_template('user/home.html')

if __name__ == "__main__":    
    app.run(debug=True)