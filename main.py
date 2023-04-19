
import pandas as pd
import numpy as np
import plotly.express as px
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
from dash import Dash, dcc, html, Input, Output


def Create_connection():
    engine = create_engine("postgresql+psycopg2://student:JvLda93aA@158.160.52.106:5432/postgres")
    conn = engine.connect()
    return conn


def Create_data():
    conn = Create_connection()
    visited = pd.read_sql(text("select visit_dttm,client_rk from msu_analytics.client order by visit_dttm"), con=conn)
    visited_by_m = visited.groupby([pd.Grouper(key='visit_dttm', freq='M')])['client_rk'].count()
    visited['visit_dttm'] = pd.to_datetime(visited['visit_dttm']) - pd.to_timedelta(7, unit='d')
    visited_by_week = visited.groupby([pd.Grouper(key='visit_dttm', freq='W')])['client_rk'].count()
    reg = pd.read_sql(text(
        """SELECT account.registration_dttm , account.account_rk
        FROM msu_analytics.client  inner join msu_analytics.account  on
        client.client_rk = account.client_rk    order by account.registration_dttm"""),
        con=conn)
    reg_by_m = reg.groupby([pd.Grouper(key='registration_dttm', freq='M')])['account_rk'].count()
    reg['registration_dttm'] = pd.to_datetime(reg['registration_dttm']) - pd.to_timedelta(7, unit='d')
    reg_by_week = reg.groupby([pd.Grouper(key='registration_dttm', freq='W')])['account_rk'].count()
    created = pd.read_sql(text("""SELECT application.application_dttm , account.account_rk
    FROM msu_analytics.account   inner join msu_analytics.application
    on account.account_rk  = application.account_rk order by application.application_dttm"""), con=conn)
    created_by_m = created.groupby([pd.Grouper(key='application_dttm', freq='M')])['account_rk'].count()
    created['application_dttm'] = pd.to_datetime(created['application_dttm']) - pd.to_timedelta(7, unit='d')
    created_by_week = created.groupby([pd.Grouper(key='application_dttm', freq='W')])['account_rk'].count()
    completed = pd.read_sql(text("""SELECT game.game_dttm  , game.game_rk, game.game_flg
     FROM msu_analytics.application  inner join msu_analytics.game
     on  application.game_rk = game.game_rk  and game.game_flg = 1  order by game.game_dttm"""), con=conn)
    completed_by_m = completed.groupby([pd.Grouper(key='game_dttm', freq='M')])['game_rk'].count()
    completed['game_dttm'] = pd.to_datetime(completed['game_dttm']) - pd.to_timedelta(7, unit='d')
    completed_by_week = completed.groupby([pd.Grouper(key='game_dttm', freq='W')])['game_rk'].count()
    return visited_by_m, visited_by_week, reg_by_m, reg_by_week, created_by_m, created_by_week, completed_by_m, completed_by_week


def Create_lists():
    visited_by_m, visited_by_week, reg_by_m, reg_by_week, created_by_m, created_by_week, completed_by_m, completed_by_week = Create_data()
    names = ["Visitors", "Registered users", "Users,created a game", "Users,participated in game"]
    month = np.array(
        [[f'{str(list(visited_by_m.keys())[i]).split(" ")[0]}'] * len(names) for i in
         range(visited_by_m.shape[0])]).reshape(1, -1)
    week = np.array(
        [[f"{str(list(visited_by_week.keys())[i]).split(' ')[0]}"] * len(names) for i in
         range(visited_by_week.shape[0])]).reshape(1, -1)
    values_m = []
    values_w = []
    new_val_m = []
    new_val_w = []
    for key in visited_by_week.keys():
        if reg_by_week.get(key) == None:
            reg_by_week[key] = 0
        if created_by_week.get(key) == None:
            created_by_week[key] = 0
        if completed_by_week.get(key) == None:
            completed_by_week[key] = 0
        values_w.append(visited_by_week[key])
        values_w.append(reg_by_week[key])
        values_w.append(created_by_week[key])
        values_w.append(completed_by_week[key])
        new_val_w.append(completed_by_week[key])
        new_val_w.append(created_by_week[key] - new_val_w[-1])
        new_val_w.append(reg_by_week[key] - new_val_w[-1] - new_val_w[-2])
        new_val_w.append(visited_by_week[key] - new_val_w[-1] - new_val_w[-2] - new_val_w[-3])
    for key in visited_by_m.keys():
        if reg_by_m.get(key) == None:
            reg_by_m[key] = 0
        if created_by_m.get(key) == None:
            created_by_m[key] = 0
        if completed_by_m.get(key) == None:
            completed_by_m[key] = 0
        values_m.append(visited_by_m[key])
        values_m.append(reg_by_m[key])
        values_m.append(created_by_m[key])
        values_m.append(completed_by_m[key])
        new_val_m.append(completed_by_m[key])
        new_val_m.append(created_by_m[key] - new_val_m[-1])
        new_val_m.append(reg_by_m[key] - new_val_m[-1] - new_val_m[-2])
        new_val_m.append(visited_by_m[key] - new_val_m[-1] - new_val_m[-2] - new_val_m[-3])
    for i in range(len(new_val_m)):
        if new_val_m[i] < 0:
            new_val_m[i] = 0
    for i in range(len(new_val_w)):
        if new_val_w[i] < 0:
            new_val_w[i] = 0
    pr_m = []
    pr_w = []
    new_m = []
    new_w = []
    for j in range(len(visited_by_week.keys())):
        pr_w.append(f'100%')
        for i in range(1, len(names)):
            if values_w[i + j * len(names) - 1] != 0:
                pr_w.append(
                    f'{int(values_w[i + j * len(names)] / values_w[i + j * len(names) - 1] * 100)}%')
                new_w.append(round(float(values_w[i + j * len(names)] / values_w[j * len(names)] * 100), 2))
            else:
                new_w.append(0)
                pr_w.append('0%')

    for j in range(len(visited_by_m.keys())):
        pr_m.append(f'100%')
        for i in range(1, len(names)):
            if values_m[i + j * len(names) - 1] != 0:
                pr_m.append(
                    f'{int(values_m[i + j * len(names)] / values_m[i + j * len(names) - 1] * 100)}%')
                new_m.append(round(float(values_m[i + j * len(names)] / values_m[j * len(names)] * 100), 2))
            else:
                new_m.append(0)
                pr_m.append('0%')
    return values_m, values_w, names, month, week, pr_m, pr_w, visited_by_m, visited_by_week, new_val_w, new_val_m, new_w, new_m


def Create_dict():
    values_m, values_w, names, month, week, pr_m, pr_w, visited_by_m, visited_by_week, new_val_w, new_val_m, new_w, new_m = Create_lists()
    new_month = []
    new_week = []
    values_m_new = []
    values_w_new = []
    for i in range(len(week[0])):
        if i % 4 == 0:
            pass
        else:
            new_week.append(week[0][i])
    for i in range(len(values_m)):
        if i % 4 == 0:
            pass
        else:
            values_m_new.append(values_m[i])
    for i in range(len(values_w)):
        if i % 4 == 0:
            pass
        else:
            values_w_new.append(values_w[i])
    for i in range(len(month[0])):
        if i % 4 == 0:
            pass
        else:
            new_month.append(month[0][i])
    data = dict(Weeks=dict(Quantity=values_w,
                           Gone_users=new_val_w,
                           Categories=names * len(visited_by_week),
                           Time_period=week[0],
                           Procents=pr_w,
                           ),
                Month=dict(Quantity=values_m,
                           Gone_users=new_val_m,
                           Categories=names * len(visited_by_m),
                           Time_period=month[0],
                           Procents=pr_m,
                           )
                )
    sec = dict(Weeks=dict(Procents=new_w,
                          Categories=names[1:] * len(visited_by_week),
                          Time_period=new_week,
                          Values=values_w_new),
               Month=dict(Procents=new_m,
                          Categories=names[1:] * len(visited_by_m),
                          Time_period=new_month,
                          Values=values_m_new))
    return data, sec


def Draw():
    colors = {
        'background': '#212121',
        'text': '#7FDBFF',
        'grid': '#124773'
    }
    external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
    app = Dash('last', external_stylesheets=external_stylesheets)
    app.title = 'Graphics for users, sorted by weeks or month'
    app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
        html.H2(children='Graphics for users, sorted by weeks or month',
                style={
                    'textAlign': 'center',
                    'color': colors['text']
                }),
        dcc.Graph(id="graph"),
        dcc.Dropdown(
            ['Weeks', 'Month'],
            'Month',
            id='demo-dropdown',
            style={'color': colors['text']}
        ),
    ])

    @app.callback(
        Output("graph", "figure"),
        Input("demo-dropdown", "value"))
    def update_line_chart(Whatperiod):
        data, sec = Create_dict()
        fig = make_subplots(rows=2, cols=1)
        fig_f = px.line(data[Whatperiod],
                        x="Time_period", y="Quantity", color='Categories', text='Procents')
        fig_s = px.bar(data[Whatperiod],
                       x="Time_period", y="Gone_users", opacity=0.3)
        fig_f.update_traces(textfont_size=11, textposition='top center')
        fig_f.add_traces(list(fig_s.select_traces()))
        for trace in fig_f.data:
            fig.add_trace(trace=trace,
                          row=1, col=1
                          )
        fig_2 = px.line(sec[Whatperiod],
                        x="Time_period", y="Procents", color='Categories', text='Values')
        fig_2.update_traces(textfont_size=11, textposition='top center')
        for trace in fig_2.data:
            fig.add_trace(row=2, col=1,
                          trace=trace)
        fig.update_layout(width=1300,
                          height=900,
                          autosize=False,
                          font_family="Courier New"
                          )
        fig['layout']['xaxis']['gridcolor'] = colors['grid']
        fig['layout']['xaxis2']['gridcolor'] = colors['grid']
        fig['layout']['yaxis']['gridcolor'] = colors['grid']
        fig['layout']['yaxis2']['gridcolor'] = colors['grid']
        fig['layout']['xaxis']['title'] = 'Временной период'
        fig['layout']['xaxis2']['title'] = 'Временной период'
        fig['layout']['yaxis']['title'] = 'Количество людей'
        fig['layout']['yaxis2']['title'] = 'Конверсия относительно общего числа посетителей'
        fig['layout']['yaxis2']['ticksuffix'] = '%'
        fig.update_layout(
            plot_bgcolor=colors['background'],
            paper_bgcolor=colors['background'],
            font_color=colors['text']
        )
        return fig

    app.run_server(debug=True, use_reloader=False)


if __name__ == '__main__':
    Draw()

 
