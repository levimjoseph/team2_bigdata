import json
import argparse
import boto3
from statsbombpy import sb


def get_competitions(s3):
    comps_raw = sb.competitions(fmt="dict")

    comps = {}
    comp_season_ids = []
    i = 0
    for c in comps_raw.values():
        comps[i] = c
        i += 1

        cid = c.get('competition_id')
        sid = c.get('season_id')
        comp_season_ids.append([cid, sid])

    if s3 is None:
        with open('../data/competitions.json', 'w') as f:
            json.dump(comps, f)
        print("Wrote competitions to local machine.")

    else:
        s3.Object('lmjose3-big-data-final',
                  'competitions.json').put(Body=json.dumps(comps))
        print("Wrote competitions to AWS s3.")

    return comp_season_ids


def get_matches(comp_season_ids, s3):
    matches = {}
    match_ids = []

    cnt = 1
    tot = len(comp_season_ids)
    for cs in comp_season_ids:
        matches = sb.matches(cs[0], cs[1], fmt="dict")

        mids = matches.keys()
        match_ids.extend(mids)

        fname = 'matches/{}_{}.json'.format(cs[0], cs[1])
        if s3 is None:
            with open('../data/{}'.format(fname), 'w') as f:
                json.dump(matches, f)
            print("Wrote {}/{} match to local machine.".format(cnt, tot))

        else:
            s3.Object('lmjose3-big-data-final',
                      fname).put(
                Body=json.dumps(matches))
            print("Wrote {}/{} match to AWS s3.".format(cnt, tot))

        cnt += 1

    return match_ids


def get_events(match_ids, s3):
    cnt = 1
    tot = len(match_ids)

    for m in match_ids:
        events = sb.events(m, fmt="dict")

        fname = 'events/{}.json'.format(m)
        if s3 is None:
            with open('../data/{}'.format(fname), 'w') as f:
                json.dump(events, f)
            print("Wrote {}/{} event to local machine.".format(cnt, tot))

        else:
            s3.Object('lmjose3-big-data-final',
                      fname).put(
                Body=json.dumps(events))
            print("Wrote {}/{} event to AWS s3.".format(cnt, tot))

        cnt += 1


def get_lineups(match_ids, s3):
    cnt = 1
    tot = len(match_ids)

    for m in match_ids:
        lineups = sb.lineups(m, fmt="dict")

        fname = 'lineups/{}.json'.format(m)
        if s3 is None:
            with open('../data/{}'.format(fname), 'w') as f:
                json.dump(lineups, f)
            print("Wrote {}/{} lineup to local machine.".format(cnt, tot))

        else:
            s3.Object('lmjose3-big-data-final',
                      fname).put(
                Body=json.dumps(lineups))
            print("Wrote {}/{} lineup to AWS s3.".format(cnt, tot))

        cnt += 1


def main():
    '''pull down data from statsbomb api'''
    parser = argparse.ArgumentParser()

    parser.add_argument("--aws", required=False)

    args = parser.parse_args()
    v = vars(args)
    aws = v["aws"]

    if aws is not None:
        s3 = boto3.resource('s3')
        print("Connected to AWS s3")
    else:
        s3 = None

    # pull down competitions
    comp_season_ids = get_competitions(s3)

    # # pull down matches
    match_ids = get_matches(comp_season_ids, s3)

    # # pull down events
    get_events(match_ids, s3)

    # # pull down lineups
    get_lineups(match_ids, s3)


if __name__ == '__main__':
    main()
