from configs.enum_headers import CandidateColumns


def get_score_features():
    return [col.value for col in _get_score_features()]

# def _get_score_features():
#     return [
#         CandidateColumns.final_score,
#         CandidateColumns.total_score,
#         CandidateColumns.projects_score,
#         CandidateColumns.midterm_score,
#         CandidateColumns.assignments_avg,
#         CandidateColumns.quizzes_avg,
#         CandidateColumns.participation_score,
#         CandidateColumns.int_grade
#     ]

def _get_score_features():
    return [
        CandidateColumns.total_score,
    ]

def get_need_normalize_features():
    return [col.value for col in _get_need_normalize_features()]

def _get_need_normalize_features():
    return [
        CandidateColumns.study_hours_per_week,
        CandidateColumns.sleep_hours_per_night,
        CandidateColumns.attendance_percent,
        CandidateColumns.stress_level,
        CandidateColumns.final_score,
        CandidateColumns.total_score,
        CandidateColumns.projects_score,
        CandidateColumns.midterm_score,
        CandidateColumns.assignments_avg,
        CandidateColumns.quizzes_avg,
        CandidateColumns.participation_score
    ]

def get_background_features_for_scoring():
    return [col.value for col in _get_background_features_for_scoring()]

def _get_background_features_for_scoring():
    return [
        CandidateColumns.study_hours_per_week,
        CandidateColumns.b_parent_edu_none,
        CandidateColumns.b_parent_edu_high_school,
        CandidateColumns.b_parent_edu_bachelor,
        CandidateColumns.b_parent_edu_master,
        CandidateColumns.b_parent_edu_phd,
        CandidateColumns.b_family_inc_high,
        CandidateColumns.b_family_inc_medium,
        CandidateColumns.b_family_inc_low,
        CandidateColumns.b_extracurricular_activities,
    ]

def get_mental_features_for_scoring():
    return [col.value for col in _get_mental_features_for_scoring()]

def _get_mental_features_for_scoring():
    return [
        CandidateColumns.sleep_hours_per_night,
        CandidateColumns.attendance_percent,
        CandidateColumns.stress_level,
        CandidateColumns.b_extracurricular_activities,
        CandidateColumns.b_internet_access_at_home
    ]

def get_feature_list_for_clustering():
    return [col.value for col in _get_feature_list_for_clustering()]

def _get_feature_list_for_clustering():
    return [
        scoring for scoring in CandidateColumns
    ]

def get_columns_for_background_cluster():
    return [col.value for col in _get_columns_for_background_cluster()]

def _get_columns_for_background_cluster():
    return [
        CandidateColumns.score_background
    ]
