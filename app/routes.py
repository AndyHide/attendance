from flask import render_template, flash, redirect, url_for, request
from app import app
from app.forms import LoginForm
from flask_login import current_user, login_user, logout_user, login_required
from app.models import User
from werkzeug.urls import url_parse
from report import Report
from datetime import datetime, timedelta
import requests


def is_day_off(day):
    if requests.get(f"http://isdayoff.ru/api/getdata?year={day.year}&month={day.month}&day={day.day}").content == b'1':
        return True
    else:
        return False


@app.route('/')
@app.route('/index')
@login_required
def index():
    day = datetime.today().strftime('%Y-%m-%d')
    report = Report('vars.json', 'et.json', 'persons.json', 'terminals.json', 'departments.json', day, 'today')
    report.connect()
    report.getEvents()
    report.calculate_attendance()
    att_list = report.prepare_person_list()
    attendance = report.attendance
    person_list = report.person_list
    return render_template('report.html', title='Home', att_list=att_list, person_list=person_list,
                           attendance=attendance, day=day, show_other=False)


@app.route('/yesterday')
@login_required
def yesterday():
    day = datetime.now() - timedelta(1)
    if is_day_off(day):
        while is_day_off(day):
            day = day - timedelta(1)
    day = datetime.strftime(day, '%Y-%m-%d')
    report = Report('vars.json', 'et.json', 'persons.json', 'terminals.json', 'departments.json', day, 'yesterday')
    report.connect()
    report.getEvents()
    report.calculate_attendance()
    att_list = report.prepare_person_list()
    attendance = report.attendance
    person_list = report.person_list
    return render_template('report.html', title='Home', att_list=att_list, person_list=person_list,
                           attendance=attendance, day=day, show_other=True)


@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user is None or not user.check_password(form.password.data):
            flash('Invalid username or password')
            return redirect(url_for('login'))
        login_user(user, remember=form.remember_me.data)
        next_page = request.args.get('next')
        if not next_page or url_parse(next_page).netloc != '':
            next_page = url_for('index')
        return redirect(next_page)
    return render_template('login.html', title='Sign In', form=form)


@app.route('/race')
@login_required
def race():
    day = datetime.now() - timedelta(1)
    if is_day_off(day):
        while is_day_off(day):
            day = day - timedelta(1)
    day = datetime.strftime(day, '%Y-%m-%d')
    report = Report('vars.json', 'et.json', 'persons.json', 'terminals.json', 'departments.json', day, 'yesterday')
    report.connect()
    report.getEvents()
    winners = report.find_winners()
    return render_template('race.html', title='Home', winners=winners, day=day)


@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('index'))
